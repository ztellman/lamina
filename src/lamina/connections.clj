;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.connections
  (:use
    [potemkin]
    [lamina.core.utils :only (assoc-record)]
    [lamina core trace])
  (:require
    [lamina.core.lock :as lock]
    [clojure.tools.logging :as log]
    [lamina.core.result :as r]))



;;;

(defn- incr-delay [delay]
  (if (zero? delay)
    4
    (min 4000 (* 2 delay))))

(defn- connection-loop [name connection-generator done? on-connected]
  (let [connection (atom nil)
        delay (atom 0)
        probe (when name (probe-channel [name :connection]))
        error-probe (when name (error-probe-channel [name :error]))
        trace #(when probe
                 (enqueue probe
                   (assoc %
                     :timestamp (System/currentTimeMillis))))]
    (run-pipeline nil
      {:error-handler (fn [err]
                        (when error-probe (enqueue error-probe err))
                        (error @connection err)
                        (swap! delay incr-delay)
                        (restart))}

      ;; check if we want to try to reconnect
      (fn [_]
        (if @done?
          (do
            (reset! connection :lamina/deactivated!)
            (complete nil))
          ;; if so, reset the connection to a fresh result-channel
          (do
            (reset! connection (result-channel))
            nil)))

      ;; wait the proscribed duration
      (wait-stage @delay)

      ;; attempt to connect
      (fn [_]
        (trace {:state :connecting})
        (connection-generator))

      ;; handle the new connection, and wait for it to close
      (fn [conn]
        (if-not (channel? conn)
          (reset! done? true)
          (do
            (when on-connected
              (try
                (on-connected conn)
                (catch Throwable e
                  (log/error e "Error in on-connected callback"))))
            (success @connection conn)
            (trace {:state :connected})
            (closed-result conn))))

      ;; handle the lost connection, and restart
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
           connection (delay (connection-loop name connection-generator latch on-connected))
           reset-fn (fn []
                      (run-pipeline @@connection
                        #(when (channel? %)
                           (close %))))
           close-fn (fn []
                      (reset! latch true)
                      (reset-fn))]
       ^{::close-fn close-fn
         ::reset-fn reset-fn}
       (fn [] @@connection))))

(defn close-connection
  "Takes a client function, and closes the connection."
  [f]
  (if-let [close-fn (-> f meta ::close-fn)]
    (close-fn)
    (f ::close)))

(defn reset-connection
  "Takes a client function, and resets the connection."
  [f]
  (if-let [reset-fn (-> f meta ::reset-fn)]
    (reset-fn)
    (f ::reset)))

;;;

(defn- try-heartbeat [options connection f]
  (when (contains? options :heartbeat)
    (let [{:keys [timeout
                  request
                  on-failure
                  response-validator
                  interval]
          :or {timeout 5000
               interval 10000}
           :as heartbeat} (:heartbeat options)]

      (when-not (contains? heartbeat :request)
        (throw (IllegalArgumentException. "heartbeat must specify :request")))

      (run-pipeline nil
        {:error-handler (fn [err]
                          (if (= :lamina/deactivated! err)
                            (complete nil)
                            (do
                              (when on-failure (on-failure err))
                              (reset-connection connection)
                              (restart))))}
        (wait-stage 0.1)
        (fn [_] (connection))
        (wait-stage interval)
        (fn [_] (f request timeout))
        (fn [response]
          (when (and response-validator
                  (not (response-validator response)))
            (throw (IllegalStateException. (str "invalid hearbeat response: " (pr-str response)))))
          (restart)))))
  f)

(defn- try-instrument [options f]
  (if (contains? options :name)
    (with-meta
      (instrument f
        (merge
          options
          {:capture :in-out}))
      (meta f))
    f))

(defmacro handle-request [[reset-fn close-fn] request & body]
  `(let [request# ~request]
     (cond
       (identical? ::close request#)
       (~close-fn)

       (identical? ::reset request#)
       (~reset-fn)

       :else
       (do ~@body))))

(deftype+ RequestTuple [request result channel])

(defn client
  "Layers a client-side request-response communication model on top of a bidirectional socket.  Returns 
   a function which takes the `request` and optionally a `timeout` in milliseconds, and returns an 
   async-promise representing the response.

   `connection-generator` is a function which takes zero parameters, and returns a channel representing
   a bidirectional socket, or an async-promise which will be realized as a bidirectional socket.  It will
   only be called once a request has been made, and whenever the socket channel subsequently closes.

   If a request is made while there is not a live connection, it will be sent once the connection is opened.
   If the connection closes while a request is pending, it will return an error unless `:retry?` is true.

   No matter how often the function is called, only one request will be in-flight at a time.  To allow request
   pipelining, see `pipelined-client`.

   Optional parameters:

     `:name` - if defined, instruments the request function, creating probes within that namespace.

     `:implicit?` - whether the instrumentation will be captured by higher-level functions, defaults to true.

     `:probes` - a map of sub-names (with `:name` implicitly prefixed) onto channels that consume those probes.
     
     `:on-connected` - a callback for when connection-generator creates a new connection, which will be invoked with
                       the new socket channel.
    
     `:executor` - an executor which will be used for response handling.

     `:retry?` - whether requests which fail due to connection failure will automatically retry.  This should only be
                 done if the requests are idempotent, and defaults to false.


  `:hearbeat` takes a configuration map, describing a periodic request that should be made to check connection health:

     `:request` - the hearbeat request.

     `:interval` - the time between heartbeat requests, defaults to ten seconds.
  
     `:timeout` - the max time spent waiting for the heartbeat response, defaults to five seconds.

     `:response-validator` - a predicate which takes a response, and returns true if it's valid.  Defaults to always returning true.

     `:on-failure` - a callback which is invoked with no parameters when the heartbeat fails."
  ([connection-generator]
     (client connection-generator nil))
  ([connection-generator
    {:keys [name
            heartbeat
            on-connected
            probes
            executor
            implicit?
            retry?]
     :or {retry? false
	      implicit? true}
     :as options}]

     (let [connection (persistent-connection
                        connection-generator
                        (select-keys options [:name :on-connected]))
           requests (channel)
           close-fn #(close-connection connection)
           reset-fn #(reset-connection connection)]

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
                               (fn [err]
                                 (when (and retry? (= :lamina/drained! err))
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
       (try-heartbeat options connection
         (try-instrument options
           ^{::close-fn close-fn
             ::reset-fn reset-fn}
           (fn
             ([request]
                (handle-request [reset-fn close-fn] request
                  (let [result (result-channel)]
                    (enqueue requests (RequestTuple. request result nil))
                    result)))
             ([request timeout]
                (handle-request [reset-fn close-fn] request
                  (let [result (expiring-result timeout)]
                    (enqueue requests (RequestTuple. request result nil))
                    result)))))))))

(defn pipelined-client
  "Like `client`, but allows request pipelining."
  ([connection-generator]
     (pipelined-client connection-generator nil))
  ([connection-generator
    {:keys [name
            heartbeat
            on-connected
            probes
            executor
            implicit?
            retry?]
     :or {retry? false}
     :as options}]

     (let [connection (persistent-connection
                        connection-generator
                        (select-keys options [:name :on-connected]))
           requests (channel)
           responses (channel)
           close-fn #(close-connection connection)
           reset-fn #(reset-connection connection)
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

       (try-heartbeat options connection
         (try-instrument options
           ^{::close-fn close-fn
             ::reset-fn reset-fn}
           (fn
             ([request]
                (handle-request [reset-fn close-fn] request
                  (let [result (result-channel)]
                    (enqueue requests (RequestTuple. request result nil))
                    result)))
             ([request timeout]
                (handle-request [reset-fn close-fn] request
                  (let [result (expiring-result timeout)]
                    (enqueue requests (RequestTuple. request result nil))
                    result)))))))))

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
  "Like `server`, but returns a function which takes a `channel` and sets up the server consumption.  

   This is slightly more efficient than calling `server` repeatedly with the same handler for different
   channels."
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
  "Models a server-side request-response communication model on top of a bidirectional socket.  For 
   each request, `handler` will be called with a `result-channel` and the `request`.  The response
   should be enqueued into the `result-channel`.

   Only one request will be pending in the handler at any time.  To pipeline requests, see `pipelined-server`.

   Optional parameters:

       `:name` -  if defined, instruments the handler function, creating probes within that namespace. This simplifies
                  the handler as a transformation of request -> response, eliding the `result-channel` and treating
                  the time spent waiting for it to become realized as part of the handler's invocation.

       `:implicit?` - whether the instrumentation will be captured by higher-level functions, defaults to true.
	
       `:probes` - a map of sub-names (with `:name` implicitly prefixed) onto channels that consume those probes.

       `:executor` - the executor for the handler.
	
       `:result-channel-generator` - a function that takes zero parameters and returns the result-channel that will be
	                                 passed into the handler.
	
       `:error-response` - the response that will be sent if there is an error in the handler."
  [handler
   ch
   {:keys
    [name
     implicit?
     probes
     executor
     result-channel-generator
     error-response]
    :or {implicit? true
	     result-channel-generator result-channel}
    :as options}]
  ((server-generator handler options) ch))

;;

(defn pipelined-server-generator
  "Like `pipelined-server`, but returns a function which takes a `channel` and sets up the server consumption.  

   This is slightly more efficient than calling `pipelined-server` repeatedly with the same handler for different
   channels."
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
          {:error-handler (fn [ex]
                            (when-not (or (drained? ch) (closed? ch))
                              (enqueue ch ex)
                              (restart)))
           :result r}
          read-channel
          (fn [response]
            (enqueue ch response)
            (restart)))

        ;; handle the requests as quickly as we can, enqueuing
        ;; the result onto the responses channel
        (receive-all ch
          (fn [request]
            (let [result (result-channel-generator)]
              (enqueue responses result)
              (handler result request))))))))

(defn pipelined-server
  "Like `server`, but can handle multiple requests from the same channel simultaneously."
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

;;;
