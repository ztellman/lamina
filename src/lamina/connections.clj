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
      (fn [_]
        (if @done?
          (do
            (reset! connection :lamina/deactivated)
            (complete nil))
          (reset! connection (result-channel)))
        nil)
      (wait-stage @delay)
      (fn [_]
        (trace {:state :connecting})
        (connection-generator))
      (fn [conn]
        (when on-connected
          (on-connected conn))
        (reset! connection conn)
        (trace {:state :connected})
        (closed-result conn))
      (fn [_]
        (trace {:state :disconnected})
        (reset! delay 0)
        (restart)))
    connection))

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
       (receive-in-order requests
         (fn [^RequestTuple r]
           (run-pipeline nil
             {:error-handler (pipeline
                               (wait-stage 1)
                               (fn [_]
                                 (when retry?
                                   (restart))))
              :result (.result r)}
             (fn [_]
               (connection))
             (fn [x]
               (if (channel? x)
                 (do
                   (enqueue x (.request r))
                   (read-channel x))
                 (error-result x))))))
       (try-instrument options
         ^{::close-fn close-fn}
         (fn
           ([request]
              (let [result (result-channel)]
                (enqueue requests (RequestTuple. request result nil))
                result))
           ([request timeout]
              (let [result (with-timeout timeout (result-channel))]
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

       (receive-all responses
         (fn [^RequestTuple r]
           (run-pipeline (.channel r)
             {:error-handler (fn [ex]
                               (if retry?
                                 (enqueue requests r)
                                 (error (.result r) ex))
                               (complete nil))}
             read-channel
             #(success (.result r) %))))

       (receive-all requests
         (fn [^RequestTuple r]
           (lock/with-exclusive-lock lock
             (when-not (r/result (.result r))
               (run-pipeline (connection)
                 {:error-handler (pipeline
                                   (wait-stage 1)
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
              (let [result (with-timeout timeout (result-channel))]
                (enqueue requests (RequestTuple. request result nil))
                result)))))))

;;;

(defn server-generator
  [f
   {:keys
    [name
     implicit?
     probes
     executor]
    :as options}]
  (let [f (fn [ch r]
            (run-pipeline nil
              (fn [_]
                (f ch r))
              (fn [_]
                ch)))]
    ))

(defn pipelined-server-generator
  [f
   {:keys
    [name
     implicit?
     probes
     executor]
    :as options}]
  )
