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
    [lamina.core.pipeline :only (success-result error-result)])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent TimeoutException]))

;;

(defn- incr-delay [delay]
  (if (zero? delay)
    500
    (min 64000 (* 2 delay))))

(defn- wait-for-close [ch description]
  (let [ch (fork ch)]
    (if (closed? ch)
      (constant-channel nil)
      (let [signal (constant-channel)]
	(receive-all ch
	  (fn [msg]
	    (when (closed? ch)
	      (log/warn (str "Connection to " description " lost."))
	      (enqueue signal nil))))
	(read-channel signal)))))

(defn- connect-loop [halt-signal connection-generator connection-callback description]
  (let [delay (atom 0)
	result (atom (error-result [nil nil]))
	latch (atom true)]
    (receive halt-signal
      (fn [_]
	(run-pipeline @result
	  #(do
	     (close %)
	     (reset! result ::close)))
	(reset! latch false)))
    (run-pipeline nil
      :error-handler (fn [val ex]
		       (swap! delay incr-delay)
		       (restart))
      (do*
      	(when (pos? @delay)
      	  (log/warn
      	    (str "Failed to connect to " description ". Waiting " @delay "ms to try again."))))
      (wait @delay)
      (fn [_] (connection-generator))
      (fn [ch]
	(when connection-callback
	  (connection-callback ch))
	(log/info (str "Connected to " description "."))
	(reset! delay 0)
	(reset! result (success-result ch))
	(wait-for-close ch description))
      (fn [_]
	(when @latch
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

;;

(defn- timeout-fn [timeout]
  (if (neg? timeout)
    (constantly 1)
    (let [final (+ (System/currentTimeMillis) timeout)]
      #(- (System/currentTimeMillis) final))))

(defn client
  ([connection-generator]
     (client connection-generator "unknown"))
  ([connection-generator description]
     (let [connection (persistent-connection connection-generator description)
	   requests (channel)]
       ;; request loop
       (receive-in-order requests
	 (fn [[request result-channel timeout]]

	   ;; set up timeout
	   (when-not (neg? timeout)
	     (run-pipeline nil
	       (wait timeout)
	       (fn [_]
		 (enqueue (:success result-channel) (TimeoutException.)))))

	   ;; make request
	   (let [timeout (timeout-fn timeout)]
	     (siphon-result
	       (run-pipeline nil
		 :error-handler (fn [_ _] (restart))
		 (fn [_]
		   (if (neg? (timeout))

		     ;; if timeout has already elapsed, don't bother
		     nil
		   
		    ;; send the request
		    (run-pipeline (connection)
		      (fn [ch]
			(if (= ::close ch)
			 
			  ;; (close-connection ...) has already been called
			  (complete (Exception. "Client has been deactivated."))
			 
			  ;; send request, and wait for response
			  (do
			    (enqueue ch request)
			    [ch (read-channel ch)]))))))
		 (fn [[ch response]]
		   (if-not (and (nil? response) (closed? ch))
		     response
		     (restart))))
	      result-channel))))

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
     ))

;;

(defn close-connection
  "Takes a client function, and closes the connection."
  [f]
  (f ::close))

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
      (when-not (closed? ch)
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
	(when-not (closed? ch)
	  (restart)))))
  (fn []
    (close ch)))
