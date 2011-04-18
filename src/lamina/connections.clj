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
    [lamina core]
    [lamina.core.channel :only (dequeue)]
    [lamina.core.pipeline :only (poll-result success-result error-result success! error!)])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent TimeoutException]
    [lamina.core.pipeline ResultChannel]))

;;

(defn- incr-delay [delay]
  (if (zero? delay)
    500
    (min 64000 (* 2 delay))))

(defn- wait-for-close [ch description]
  (let [ch (fork ch)]
    (if (drained? ch)
      (constant-channel nil)
      (let [signal (constant-channel)]
	(receive-all ch
	  (fn [msg]
	    (when (drained? ch)
	      (log/warn (str "Connection to " description " lost."))
	      (enqueue signal nil))))
	(read-channel signal)))))

(defn- connect-loop [halt-signal connection-generator connection-callback description]
  (let [delay (atom 0)
	result (atom (result-channel))
	latch (atom true)]
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
      	  (log/warn
      	    (str "Failed to connect to " description ". Waiting " @delay "ms to try again."))))
      (wait-stage @delay)
      (fn [_]
	(siphon-result
	  (connection-generator)
	  @result))
      (fn [ch]
	(when connection-callback
	  (connection-callback ch))
	(log/info (str "Connected to " description "."))
	(wait-for-close ch description))
      ;; wait here for connection to drop
      (fn [_]
	(when @latch
	  (reset! delay 0)
	  (reset! result (result-channel))
	  (restart))))
    result))

(defn persistent-connection
  ([connection-generator]
     (persistent-connection connection-generator "unknown"))
  ([connection-generator description]
     (persistent-connection connection-generator description nil))
  ([connection-generator description connection-callback]
     (let [close-signal (constant-channel)
	   result (connect-loop close-signal connection-generator connection-callback description)]
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

;;

(defn- has-completed? [result-ch]
  (wait-for-message (poll-result result-ch 0)))

(defn- parametrized-wait [wait-millis]
  (run-pipeline wait-millis
    (wait-stage wait-millis)))

(defn setup-result-timeout [result timeout]
  (when-not (neg? timeout)
    (receive (poll-result result timeout)
      #(when-not %
	 (error! result (TimeoutException. (str "Timed out after " timeout "ms.")))))))

(defn client
  ([connection-generator]
     (client connection-generator "unknown"))
  ([connection-generator description]
     (let [connection (persistent-connection connection-generator description)
	   requests (channel)]
       ;; request loop
       (receive-in-order requests
	 (fn [[request ^ResultChannel result timeout]]

	   (if (= ::close request)
	     (close-connection connection)
	     (do
	       ;; set up timeout
	       (setup-result-timeout result timeout)

	       ;; make request
	       (siphon-result
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
			     (throw response)
			     response)
			   (restart))))))
		 result)))))

       ;; request function
       (fn this
	 ([request]
	    (this request -1))
	 ([request timeout]
	    (let [result (result-channel)]
	      (enqueue requests [request result timeout])
	      result))))))

(defn pipelined-client
  ([connection-generator]
     (pipelined-client connection-generator "unknown"))
  ([connection-generator description]
     (let [connection (persistent-connection connection-generator description)
	   requests (channel)
	   responses (channel)]

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
	 (fn [[request ^ResultChannel result ch]]
	   (run-pipeline ch
	     :error-handler (fn [_]
			      ;; re-send request
			      (when-not (has-completed? result)
				(enqueue requests [request result -1]))
			      (complete nil))
	     read-channel
	     (fn [response]
	       (if (and (nil? response) (drained? ch))
		 (throw (Exception. "Connection closed"))
		 (if (instance? Exception response)
		   (error! result response)
		   (success! result response)))))))

       ;; request function
       (fn this
	 ([request]
	    (this request -1))
	 ([request timeout]
	    (let [result (result-channel)]
	      (enqueue requests [request result timeout])
	      result))))))

;;

(defn server
  [ch handler]
  (run-pipeline ch
    read-channel
    #(let [c (constant-channel)]
       (handler c %)
       (read-channel c))
    #(enqueue ch %)
    (fn [_]
      (when-not (drained? ch)
	(restart))))
  (fn []
    (close ch)))

(defn pipelined-server
  [ch handler]
  (let [requests (channel)
	responses (channel)]
    (run-pipeline responses
      read-channel
      #(read-channel %)
      #(enqueue ch %)
      (fn [_] (restart)))
    (run-pipeline ch
      read-channel
      #(let [c (constant-channel)]
	 (handler c %)
	 (enqueue responses c))
      (fn [_]
	(when-not (drained? ch)
	  (restart)))))
  (fn []
    (close ch)))
