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

(defn- incr-delay [delay]
  (if (zero? delay)
    500
    (min 64000 (* 2 delay))))

(defn- wait-for-close
  "Returns a result-channel representing the closing of the channel."
  [ch options]
  (run-pipeline (closed-result ch)
    (fn [_]
      (trace [(:name options) :connection :lost]
	(select-keys options [:name :description]))
      true)))

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
	  (reset! result ::close)
	  (reset! latch false)
	  (run-pipeline connection close))))

    ;; run connection loop
    (run-pipeline nil
      :error-handler (fn [ex]
		       (swap! delay incr-delay)
		       (reset! result (result-channel))
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
	    (success! @result ch)
	    (wait-for-close ch options))))
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

(defn- has-completed? [result-ch]
  (wait-for-message (poll-result result-ch 0)))

(defn setup-result-timeout [result timeout]
  (when-not (neg? timeout)
    (receive (poll-result result timeout)
      #(when-not %
	 (error! result (TimeoutException. (str "Timed out after " timeout "ms.")))))))

(defn client
  ([connection-generator]
     (client connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (str (gensym "client:"))
		      :description "unknown"}
		     options)
	   connection (persistent-connection connection-generator options)
	   requests (channel)
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
	       ;; set up timeout
	       (setup-result-timeout result timeout)

	       ;; make request
	       (run-pipeline nil ;; don't wait anything initially
		 :error-handler (fn [_]
				  (when-not (has-completed? result)
				    (restart)))
		   
		 (fn [_]
		   (if (has-completed? result)
		       
		     ;; if timeout has already elapsed, exit
		     (complete nil)

		     ;; send the request
		     (run-pipeline (connection)
		       (fn [ch]
			 (if (= ::close ch)

			   ;; (close-connection ...) has already been called
			   (complete (Exception. "Client has been deactivated."))

			   ;; send request, and wait for response
			   (do
			     (enqueue ch request)
			     ch))))))
		 (fn [ch]
		   (run-pipeline ch
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
		result)))
	 options))))

(defn pipelined-client
  ([connection-generator]
     (pipelined-client connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (str (gensym "client:"))
		      :description "unknown"}
		     options) 
	   connection (persistent-connection connection-generator options)
	   requests (channel)
	   responses (channel)]

       (siphon-probes (:name options) (:probes options))

       ;; handle requests
       (receive-in-order requests
	 (fn [[request ^ResultChannel result timeout]]
	   (if (= ::close request)
	     (do
	       (close-connection connection)
	       (success! result true))
	     (do
	       ;; setup timeout
	       (setup-result-timeout result timeout)

	       ;; send requests
	       (run-pipeline nil 
                 :error-handler (fn [ex]
                                  (if-not (has-completed? result)
				    (restart)
                                    (complete nil)))
                 (fn [_] (connection))
		 (fn [ch]
                   (if (= ::close ch)
		     (error! result (Exception. "Client has been deactivated."))
		     (when-not (has-completed? result)
		       (enqueue ch request)
		       (enqueue responses [request result ch])))))))))

       ;; handle responses
       (receive-in-order responses
	 (fn [[request result ch]]
	   (run-pipeline ch
	     :error-handler (fn [_]
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
    (let [ch (channel-generator)]
      (f ch x)
      (read-channel ch))))

(defn server
  ([ch handler]
     (server ch handler {}))
  ([ch handler options]
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
	     (close ch)))))
     nil))

(defn pipelined-server
  ([ch handler]
     (pipelined-server ch handler {}))
  ([ch handler options]
     (let [options (merge
		     {:name (str (gensym "server:"))}
		     options)
	   thread-pool (let [t (:thread-pool options)]
			 (if (or (nil? t) (thread-pool? t))
			   t
			   (thread-pool t)))
	   include-request? (:include-request options)
	   response-channel-generator (or (:response-channel options) constant-channel)
	   requests (channel)
	   responses (channel)
	   handler (executor thread-pool
		     (fn [req]
		       (let [ch (response-channel-generator)]
			 (handler ch req)
			 (read-channel ch)))
		     options)]

       (siphon-probes (:name options) (:probes options))
       
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
	     (close ch)))))
     nil))
