;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:author "Zachary Tellman"}
  lamina.connections
  (:use
    [lamina core executors api trace])
  (:import
    [java.util.concurrent TimeoutException]
    [lamina.core.pipeline ResultChannel]))

;;

(defn- has-completed? [result-ch]
  (wait-for-message (poll-result result-ch 0)))

(defn- incr-delay [delay]
  (if (zero? delay)
    125
    (min 4000 (* 2 delay))))

(defn- wait-for-close
  "Returns a result-channel representing the closing of the channel."
  [ch]
  (closed-result ch))

(defn- connect-loop
  "Continually reconnects to server. Returns an atom which will always contain a result-channel
   for the latest attempted connection."
  [halt-signal connection-generator options]
  (let [delay (atom 0)
	result (atom (result-channel))
	latch (atom true)
	probe-prefix (:name options)
	timestamp (ref (System/nanoTime))
	elapsed #(dosync
		   (let [last-time (ensure timestamp)
			 current-time (System/nanoTime)]
		     (ref-set timestamp current-time)
		     (/ (- current-time last-time) 1e6)))
	desc (select-keys options [:name :description])]
    ;; handle signal to close persistent connection
    (receive halt-signal
      (fn [_]
	(let [connection @result]
          (success! @result ::close)
          (reset! result (success-result ::close))
	  (reset! latch false)
	  (run-pipeline connection #(when (channel? %) (close %))))))

    ;; run connection loop
    (run-pipeline nil
      :error-handler (fn [ex]
		       (swap! delay incr-delay)
		       (when (and (result-channel? @result) (has-completed? @result))
                         (reset! result (result-channel)))
		       (if @latch
			 (restart)
			 (complete nil)))
      (do-stage
      	(when (pos? @delay)
	  (trace [probe-prefix :connection:failed] (merge desc {:event :connect-attempt-failed, :delay @delay}))))
      (wait-stage @delay)
      (fn [_]
	(trace [probe-prefix :connection:attempting] (assoc desc :event :attempting-connection))
	(connection-generator))
      (fn [ch]
	(trace [probe-prefix :connection:opened] (assoc desc :event :connection-opened, :elapsed (elapsed)))
	(run-pipeline
	  (when-let [new-connection-callback (:connection-callback options)]
	    (new-connection-callback ch))
	  (fn [_]
	    (future (success! @result ch))
            (Thread/yield)
            (when-not @latch (close ch))
            (wait-for-close ch))))
      ;; wait here for connection to drop
      (fn [_]
	(trace [probe-prefix :connection:lost] (assoc desc :event :connection-lost, :elapsed (elapsed)))
	(when @latch
	  (reset! delay 0)
	  (reset! result (result-channel))
	  (restart))))
    result))

(defn persistent-connection
  ([connection-generator]
     (persistent-connection connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (str (gensym "connection:")), :description "unknown"}
		     options)
	   close-signal (constant-channel)
	   result (connect-loop close-signal connection-generator options)]
       (fn
	 ([]
	    @result)
	 ([signal]
	    (when (= ::close signal)
	      (enqueue close-signal nil)))))))

(defn close-connection
  "Takes a client function, and closes the connection."
  [f]
  (if-let [close-fn (-> f meta ::close-fn)]
    (close-fn)
    (f ::close)))

;;;

(defn- setup-result-timeout [result timeout]
  (when-not (neg? timeout)
    (receive (poll-result result timeout)
      #(when-not %
	 (error! result (TimeoutException. (str "Timed out after " timeout "ms.")))))))

(defn- setup-connection-timeout [result ch]
  (run-pipeline result
    :error-handler
    (fn [ex]
      (when (instance? TimeoutException ex)
	(close ch)))))

(defn client
  "Given a function that returns a bi-directional channel, returns a function that
   accepts (f request timeout?) and returns a result-channel representing the eventual
   response."
  ([connection-generator]
     (client connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (str (gensym "client:"))
		      :description "unknown"
		      :reconnect-on-timeout? true}
		     options)
	   reconnect-on-timeout? (:reconnect-on-timeout? options)
	   connection (persistent-connection connection-generator options)
	   requests (channel)
	   pause? (atom false)
	   closed? (atom false)]

       (siphon-probes (:name options) (:probes options))
       
       ;; request loop
       (receive-in-order requests
	 (fn [[request ^ResultChannel result timeout]]

	   (if (= ::close request)
	     (do
	       (close-connection connection)
	       (success! result true))
	     (do
	       ;; make request
	       (run-pipeline nil
		 :error-handler (fn [ex]
				  (reset! pause? true)
				  (if-not (has-completed? result)
				    (restart)
				    (complete nil)))

		 (fn [_]
		   (when (compare-and-set! pause? true false)
		     (run-pipeline nil
		       (wait-stage 1))))
		   
		 (fn [_]

		   (if (has-completed? result)
		       
		     ;; if timeout has already elapsed, exit
		     (complete nil)
		     
		     ;; send the request
		     (run-pipeline
		       (connection)
		       (fn [ch]
			 (if (= ::close ch)

			   ;; (close-connection ...) has already been called
			   (complete (Exception. "Client has been deactivated."))

			   ;; send request, and wait for response
			   (do
			     (when reconnect-on-timeout?
			       (setup-connection-timeout result ch))
			     (enqueue ch request)
			     ch))))))
		 (fn [ch]
		   (run-pipeline ch
		     :error-handler (fn [_] )

		     read-channel

		     (fn [response]
		       (if-not (and (nil? response) (drained? ch))
			 (if (instance? Exception response)
			   (error! result response)
			   (success! result response))
			 (throw (Exception. "Connection unexpectedly closed.")))))))))))

       ;; request function
       (trace-wrap
	 (fn this
	   ([request]
	      (this request -1))
	   ([request timeout]
	      (let [result (result-channel)]
		(enqueue requests [request result timeout])
		(setup-result-timeout result timeout)
		result)))
	 options))))

(defn pipelined-client
  "Given a function that returns a bi-directional channel, returns a function that
   accepts (f request timeout?) and returns a result-channel representing the eventual
   response.

   Requests are sent immediately, under the assumption that responses will be sent in the same
   order.  This is not true of all servers/protocols."
  ([connection-generator]
     (pipelined-client connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (str (gensym "client:"))
		      :description "unknown"
		      :reconnect-on-timeout? true}
		     options)
	   reconnect-on-timeout? (:reconnect-on-timeout? options)
	   connection (persistent-connection connection-generator options)
	   requests (channel)
	   pause? (atom false)
	   responses (channel)]

       (siphon-probes (:name options) (:probes options))

       ;; handle requests
       (receive-in-order requests
	 (fn [[request ^ResultChannel result timeout]]
	   (if (= ::close request)
	     (do
	       (close-connection connection)
	       (success! result true))
	     (when-not (has-completed? result)

	       ;; send requests
	       (run-pipeline nil 
                 :error-handler (fn [ex]
				  (reset! pause? true)
				  (if-not (has-completed? result)
				    (restart)
                                    (complete nil)))
		 (fn [_]
		   (when (compare-and-set! pause? true false)
		     (run-pipeline nil
		       (wait-stage 1))))
                 (fn [_]
		   (connection))
		 (fn [ch]
		   (when reconnect-on-timeout?
		     (setup-connection-timeout result ch))
                   (if (= ::close ch)
		     (error! result (Exception. "Client has been deactivated."))
		     (when-not (has-completed? result)
		       (enqueue ch request)
		       (enqueue responses [request result ch])))))))))

       ;; handle responses
       (receive-in-order responses
	 (fn [[request result ch]]
	   (run-pipeline ch
	     :error-handler (fn [ex]
			      ;; re-send request
			      (when-not (has-completed? result)
				(enqueue requests [request result -1]))
			      (complete nil))
	     read-channel
	     (fn [response]
	       (if (and (nil? response) (drained? ch))
		 (throw (Exception. "Connection unexpectedly closed."))
		 (if (instance? Exception response)
		   (error! result response)
		   (success! result response)))))))

       ;; request function
       (trace-wrap
	 (fn this
	   ([request]
	      (this request -1))
	   ([request timeout]
	      (let [result (result-channel)]
		(setup-result-timeout result timeout)
		(enqueue requests [request result timeout])
		result)))
	 options))))


(defn client-pool
  "Returns a client function that distributes requests across n-many clients."
  [n client-generator]
  (let [clients (take n (repeatedly client-generator))
	counter (atom 0)]
    (fn this
      ([request]
	 (this request -1))
      ([request timeout]
	 (if (= ::close request)
	   (async (force-all (map close-connection clients)))
	   (let [idx (swap! counter #(rem (inc %) n))
		 client (nth clients idx)]
	     (client request timeout)))))))

;;;

(defn- wrap-constant-response [f channel-generator options]
  (fn [x]
    (let [result (result-channel)
	  ch (channel-generator)]
      (run-pipeline nil
	:error-handler #(error! result %)
	(fn [_] (f ch x)))
      (siphon-result
	(read-channel ch)
	result))))

(defn server-generator
  "Given a handler function, returns a function that takes a bi-directional channel
   and consumes requests one at a time, passing them to the handler function with
   a response channel.  Requests will be handled one at a time.

   The handler function should accept two parameters, with the pattern (handler ch request)."
  ([handler]
     (server-generator handler {}))
  ([handler options]
     (let [options (merge
		     {:name (str (gensym "server:"))}
		     options)
	   thread-pool (let [t (:thread-pool options)]
			 (if (or (nil? t) (thread-pool? t))
			   t
			   (thread-pool t)))
	   include-request? (:include-request options)
	   handler (executor thread-pool
		     (wrap-constant-response handler
		       (or (:response-channel options) constant-channel)
		       options)
		     options)]

       (siphon-probes (:name options) (:probes options))

       (fn [ch]
         (run-pipeline ch
           read-channel
           (fn [request]
             (when-not (and (nil? request) (drained? ch))
               (run-pipeline request
                 :error-handler #(complete
                                   (enqueue ch
                                     (if include-request?
                                       {:request request, :response %}
                                       %)))
                 #(handler [%])
                 #(enqueue ch
                    (if include-request?
                      {:request request, :response %}
                      %)))))
           (fn [_]
             (if-not (drained? ch)
               (restart)
               (close ch))))))))

(defn server
  "Given a handler function and a bi-directional channel, consumes requests one at a time,
   passing them to the handler function a response channel.  Requests will be handled one
   at a time.

   The handler function should accept two parameters, with the pattern (handler ch request)."
  ([ch handler]
     (server ch handler {}))
  ([ch handler options]
     ((server-generator handler options) ch)))

(defn pipelined-server-generator
  "Given a handler function, returns a function that takes a bi-directional channel
   and consumes requests one at a time, passing them to the handler function with
   a response channel.  Requests will be handled in parallel, but responses will be
   sent in-order.

   The handler function should accept two parameters, with the pattern (handler ch request)."
  ([handler]
     (pipelined-server-generator handler {}))
  ([handler options]
     (let [options (merge
		     {:name (str (gensym "server:"))}
		     options)
	   thread-pool (let [t (:thread-pool options)]
			 (if (or (nil? t) (thread-pool? t))
			   t
			   (thread-pool t)))
	   include-request? (:include-request options)
	   handler (executor thread-pool
		     (wrap-constant-response handler
		       (or (:response-channel options) constant-channel)
		       options)
		     options)]

       (siphon-probes (:name options) (:probes options))
       
       (fn [ch]

         (let [requests (channel)
               responses (channel)]

           (run-pipeline responses
             read-channel
             #(enqueue ch %)
             (fn [_] (restart)))
         
           (run-pipeline ch
             read-channel
             (fn [request]
               (when-not (and (nil? request) (drained? ch))
                 (run-pipeline request
                   :error-handler #(complete
                                     (enqueue responses
                                       (if include-request?
                                         {:request request, :response %}
                                         %)))
                   #(handler [%])
                   (fn [c]
                     (if include-request?
                       {:request request, :response c}
                       c))
                   #(enqueue responses %))))
             (fn [_]
               (if-not (drained? ch)
                 (restart)
                 (close ch)))))))))

(defn pipelined-server
  "Given a handler function and a bi-directional channel, consumes requests one at a time,
   passing them to the handler function a response channel.  Requests will be handled in
   parallel, but responses will be sent in-order.

   The handler function should accept two parameters, with the pattern (handler ch request)."
  ([ch handler]
     (pipelined-server ch handler {}))
  ([ch handler options]
     ((pipelined-server-generator handler options) ch)))
