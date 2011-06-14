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
		       (restart))
      (do-stage
      	(when (pos? @delay)
	  (trace [probe-prefix :connection :failed] (merge desc {:delay @delay}))))
      (wait-stage @delay)
      (fn [_]
	(trace [probe-prefix :connection :attempted] desc)
	(siphon-result
	  (connection-generator)
	  @result))
      (fn [ch]
	(trace [probe-prefix :connection :opened] desc)
	(wait-for-close ch options))

      ;; wait here for connection to drop
      (fn [_]
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
		     {:name (gensym "connection."), :description "unknown"}
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
		     {:name (gensym "client.")
		      :description "unknown"}
		     options)
	   connection (persistent-connection connection-generator options)
	   requests (channel)]

       (siphon-probes (:name options) (:probes options))
       
       ;; request loop
       (receive-in-order requests
	 (fn [[request ^ResultChannel result timeout]]

	   (if (= ::close request)
	     (close-connection connection)
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
			 (throw (Exception. "Connection unexpectedly closed.")))))))

	       result))))

       ;; request function
       (trace-wrap
	 (fn this
	   ([request]
	      (this request -1))
	   ([request timeout]
	      (let [result (result-channel)]
		(enqueue requests [request result timeout])
		result)))
	 (merge
	   {:name (gensym "client.")}
	   options)))))

(defn pipelined-client
  ([connection-generator]
     (pipelined-client connection-generator nil))
  ([connection-generator options]
     (let [options (merge
		     {:name (gensym "client.")
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
	     (close-connection connection)
	     (do
	       ;; setup timeout
	       (setup-result-timeout result timeout)

	       ;; send requests
	       (run-pipeline nil 
                 :error-handler (fn [_]
                                  (if-not (has-completed? result)
				    (restart)
                                    (complete nil)))
                 (fn [_] (connection))
		 (fn [ch]
                   (when-not (has-completed? result)
                     (enqueue ch request)
                     (enqueue responses [request result ch]))))))))

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
	 (merge
	   {:name (gensym "client.")}
	   options)))))

;;

(defn- wrap-constant-response [f options]
  (fn [x]
    (let [ch (if-let [response-channel-generator (:response-channel options)]
	       (response-channel-generator)
	       (constant-channel))]
      (f ch x)
      (read-channel ch))))

(defn server
  ([ch handler]
     (server ch handler {}))
  ([ch handler options]
     (let [options (merge
		     {:name (gensym "server.")}
		     options)
	   thread-pool (let [t (:thread-pool options)]
			 (if (or (nil? t) (thread-pool? t))
			   t
			   (thread-pool t)))
	   timeout-fn (or (:timeout options) (constantly -1))
	   handler (executor thread-pool (wrap-constant-response handler options) options)]

       (siphon-probes (:name options) (:probes options))

       (run-pipeline ch
	 :error-handler #(do
			   (enqueue ch (or (:error-response options) %))
			   (when-not (drained? ch)
			     (restart)))
	 read-channel
	 (fn [request]
	   (when-not (and (nil? request) (drained? ch))
	     (run-pipeline request
	       #(handler [%] {:timeout (timeout-fn %)})
	       #(enqueue ch %))))
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
		     {:name (gensym "server.")}
		     options)
	   thread-pool (let [t (:thread-pool options)]
			 (if (or (nil? t) (thread-pool? t))
			   t
			   (thread-pool t)))
	   timeout-fn (or (:timeout options) (constantly -1))
	   handler (executor thread-pool handler options)
	   requests (channel)
	   responses (channel)]

       (siphon-probes (:name options) (:probes options))
       
       (run-pipeline responses
	 read-channel
	 #(read-channel %)
	 #(enqueue ch %)
	 (fn [_] (restart)))
       (run-pipeline ch
	 read-channel
	 (fn [request]
	   (when-not (and (nil? request) (drained? ch))
	     (run-pipeline request
	       (fn [request]
		 (let [c (if-let [response-channel-generator (:response-channel options)]
			   (response-channel-generator)
			   (constant-channel))]
		   (run-pipeline request
		     :error-handler #(redirect (pipeline (constant-channel %)))
		     #(handler [c %] {:timeout (timeout-fn %)})
		     (fn [_] c))))
	       #(enqueue responses %))))
	 (fn [_]
	   (if-not (drained? ch)
	     (restart)
	     (close ch)))))
     nil))
