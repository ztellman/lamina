;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.connections
  (:use
    [useful.datatypes :only (assoc-record)]
    [lamina core trace])
  (:require
    [lamina.core.lock :as lock]
    [clojure.tools.logging :as log]
    [lamina.core.result :as r]))

(set! *warn-on-reflection* true)

;;;

(defn- incr-delay [delay]
  (if (zero? delay)
    4
    (min 4000 (* 2 delay))))

(defn- connection-loop [name connection-generator done? on-connected]
  (let [connection (atom nil)
        delay (atom 0)
        probe (when name (probe-channel [name :connection]))
        trace #(when probe
                 (enqueue probe
                   (assoc %
                     :timestamp (System/currentTimeMillis))))]
    (run-pipeline nil
      {:error-handler (fn [err]
                        (error @connection err)
                        (swap! delay incr-delay)
                        (restart))}

      ;; check if we want to try to reconnect
      (fn [_]
        (if @done?
          (do
            (reset! connection :lamina/deactivated)
            (complete nil))
          ;; if so, reset the connection to a fresh result-channel
          (reset! connection (result-channel)))
        nil)

      ;; wait the proscribed duration
      (wait-stage @delay)

      ;; attempt to connect
      (fn [_]
        (trace {:state :connecting})
        (connection-generator))

      ;; handle the new connection, and wait for it to close
      (fn [conn]
        (when on-connected
          (on-connected conn))
        (reset! connection conn)
        (trace {:state :connected})
        (closed-result conn))

      ;; handle the lost connection, and restart
      (fn [_]
        (trace {:state :disconnected})
        (reset! delay 0)
        (restart)))
    connection))

;; TODO: make the connection lazy
(defn persistent-connection
  ([connection-generator]
     (persistent-connection connection-generator nil))
  ([connection-generator {:keys [name on-connected]}]
     (let [latch (atom false)
           connection (connection-loop name connection-generator latch on-connected)
           close-fn (fn []
                      (reset! latch false)
                      (close @connection))]
       ^{::close-fn close-fn} (fn [] @connection))))

(defn close-connection
  "Takes a client function, and closes the connection."
  [f]
  (if-let [close-fn (-> f meta ::close-fn)]
    (close-fn)
    (f ::close)))

;;;

(defn try-instrument [options f]
  (if (contains? options :name)
    (instrument f options)
    f))

(deftype RequestTuple [request result channel])

(defn client
  ([connection-generator]
     (client connection-generator nil))
  ([connection-generator
    {:keys [name
            on-connected
            probes
            executor
            implicit?
            retry?]
     :or {retry? true}
     :as options}]

     (let [connection (persistent-connection
                        connection-generator
                        (select-keys options [:name :on-connected]))
           requests (channel)
           close-fn #(close-connection connection)]

       ;; consume the requests, one at a time
       (run-pipeline requests
         {:error-handler (fn [_] (restart))}
         read-channel
         (fn [^RequestTuple r]
           (run-pipeline nil
             ;; if we have a request error, wait a bit so we don't starve
             ;; the connect loop, then resend the request if :retry? is true
             {:error-handler (pipeline
                               (wait-stage 0.1)
                               (fn [_]
                                 (when retry?
                                   (restart))))
              :result (.result r)}
             ;; connect
             (fn [_]
               (connection))
             ;; enqueue the request, then wait for the response
             (fn [x]
               (if (channel? x)
                 (do
                   (enqueue x (.request r))
                   (read-channel x))
                 (error-result x)))))
         ;; rinse, repeat
         (fn [_]
           (restart)))
       (try-instrument options
         ^{::close-fn close-fn}
         (fn
           ([request]
              (let [result (result-channel)]
                (enqueue requests (RequestTuple. request result nil))
                result))
           ([request timeout]
              (let [result (expiring-result timeout)]
                (enqueue requests (RequestTuple. request result nil))
                result)))))))

(defn pipelined-client
  ([connection-generator]
     (pipelined-client connection-generator nil))
  ([connection-generator
    {:keys [name
            on-connected
            probes
            executor
            implicit?
            retry?]
     :or {retry? true}
     :as options}]

     (let [connection (persistent-connection
                        connection-generator
                        (select-keys options [:name :on-connected]))
           requests (channel)
           responses (channel)
           close-fn #(close-connection connection)
           lock (lock/lock)]

       ;; consume the responses from the channel, and pair them with
       ;; the result-channels in the RequestTuple
       (receive-all responses
         (fn [^RequestTuple r]
           (run-pipeline (.channel r)
             ;; if we fail and :retry? is true, resend the request
             {:error-handler (fn [ex]
                               (if retry?
                                 (enqueue requests r)
                                 (error (.result r) ex))
                               (complete nil))}
             read-channel
             #(success (.result r) %))))

       ;; send the request, and enqueue the result and connection onto
       ;; the response channel
       (receive-all requests
         (fn [^RequestTuple r]
           (lock/with-exclusive-lock lock
             (when-not (r/result (.result r))
               (run-pipeline (connection)
                 ;; if we fail and :retry? is true, restart the pipeline
                 {:error-handler (pipeline
                                   (wait-stage 0.1)
                                   (fn [ex]
                                     (if retry?
                                       (restart)
                                       (do
                                         (error (.result r) ex)
                                         (complete nil)))))}
                 (fn [x]
                   (if (channel? x)
                     (do
                       (enqueue x (.request r))
                       (enqueue responses (assoc-record r :channel x)))
                     (error (.result r) x))))))))

       (try-instrument options
         ^{::close-fn close-fn}
         (fn
           ([request]
              (let [result (result-channel)]
                (enqueue requests (RequestTuple. request result nil))
                result))
           ([request timeout]
              (let [result (expiring-result timeout)]
                (enqueue requests (RequestTuple. request result nil))
                result)))))))

;;;

(defn server-generator-
  [handler-fn
   {:keys
    [result-channel-generator
     error-response
     timeout]
    :or {result-channel-generator result-channel}
    :as options}
   server-fn]
  (let [f (fn this
            ([request]
               (this (result-channel-generator) request))
            ([ch request]
               (let [ch (if timeout
                          (with-timeout (timeout request) ch)
                          ch)]
                 (run-pipeline nil
                   {:error-handler (fn [ex]
                                     (if error-response
                                       (success ch (error-response ex))
                                       (error ch ex)))}
                   (fn [_]
                     (handler-fn ch request)))
                 ch)))
        f (try-instrument options f)]
    (fn [ch]
      (server-fn f ch))))

;;

(defn server-generator
  [handler
   {:keys
    [name
     implicit?
     probes
     executor
     result-channel-generator
     timeout
     error-response]
    :as options}]
  (server-generator- handler options
    (fn [handler ch]
      (run-pipeline ch
        {:error-handler (fn [ex]
                          (when-not (or (drained? ch) (closed? ch))
                            (enqueue ch ex)
                            (restart)))}
        read-channel
        handler
        (fn [response]
          (enqueue ch response)
          (when-not (or (drained? ch) (closed? ch))
            (restart)))))))

(defn server
  [handler
   ch
   {:keys
    [name
     implicit?
     probes
     executor
     result-channel-generator
     error-response]
    :as options}]
  ((server-generator handler options) ch))

;;

(defn pipelined-server-generator
  [handler
   {:keys
    [name
     implicit?
     probes
     executor
     result-channel-generator
     timeout
     error-response]
    :or {result-channel-generator result-channel}
    :as options}]
  (server-generator- handler options
    (fn [handler ch]
      (let [responses (channel)
            r (result-channel)]

        ;; wait for the responses, returning them in-order
        (run-pipeline responses
          {:error-handler #(enqueue ch %)
           :result r}
          read-channel
          (fn [response]
            (enqueue ch response)
            (when-not (or (drained? ch) (closed? ch))
              (restart))))

        ;; handle the requests as quickly as we can, enqueuing
        ;; the result onto the responses channel
        (receive-all ch
          (fn [request]
            (let [result (result-channel-generator)]
              (enqueue responses result)
              (handler result request))))))))

(defn pipelined-server
  [handler
   ch
   {:keys
    [name
     implicit?
     probes
     executor
     result-channel-generator
     error-response]
    :as options}]
  ((pipelined-server-generator handler options) ch))
