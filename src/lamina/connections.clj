;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.connections
  (:use
    [lamina core]
    [lamina.core.channel :only (wait-channel)])
  (:require
    [clojure.contrib.logging :as log]))

;;

(defn reconnect-loop [connection-generator result]
  (let [delay (atom 500)]
    (run-pipeline nil
      :error-handler (fn [_ _]
		       (log/info
			 (str
			   "Waiting "
			   (swap! delay #(min 64000 (* % 2)))
			   "ms before attempting reconnection."))
		       (restart))
      (fn [_] (read-channel (wait-channel @delay)))
      (fn [_] (connection-generator))
      (fn [ch]
	(let [[a b] (fork 2 ch)]
	  (enqueue (:success result) a)
	  (receive-all b
	    (fn [_]
	      (when (closed? b)
		(enqueue (:error result) [b (Exception. "Connection severed.")])))))))))

(defn persistent-connection [connection-generator]
  (let [error-result (pipeline-channel
		       nil-channel
		       (constant-channel [nil nil]))
	connection (atom error-result)
	delay (atom 500)]
    (fn []
      (let [c @connection]
	(run-pipeline c
	  :error-handler
	  (fn [_ ex]
	    (log/warn "lamina.connections" ex)
	    (let [c* (pipeline-channel)]
	      (println c c*)
	      (if (compare-and-set! connection c c*)
		(do
		  (reconnect-loop connection-generator c*)
		  (redirect (pipeline (constantly c*)) nil))
		@connection))))))))

;;

(defn- timeout-fn [timeout]
  (if (neg? timeout)
    (constantly timeout)
    (let [now (System/currentTimeMillis)]
      #(max 0 (- (+ now timeout) (System/currentTimeMillis))))))

(defn- start-sync-request-handler [connection-generator requests]
  (run-pipeline requests
    read-channel
    (fn [[request response timeout]]
      (run-pipeline (connection-generator)
	:error-handler (fn [_ _]
			 (if-not (zero? (timeout))
			   (restart)
			   (complete nil)))
	(fn [ch]
	  (enqueue ch request)
	  (run-pipeline (read-channel ch (timeout))
	    :error-handler (fn [_ ex]
			     (enqueue (:error response) [request ex])
			     (complete
			       (when-not (closed? ch)
				 #(read-channel ch))))
	    #(do
	       (if (closed? ch)
		 (throw (Exception. "Request connection severed."))
		 (enqueue (:success response) %))
	       nil)))
	#(when % (%))))
    (fn [_] (restart))))

(defn request-handler [connection-generator]
  (let [requests (channel)]
    (start-sync-request-handler connection-generator requests)
    (fn this
      ([request]
	 (this request -1))
      ([request timeout]
	 (let [response (pipeline-channel)]
	   (enqueue requests [request response (timeout-fn timeout)])
	   response)))))

;;

(defn start-pipelined-request-handler [response-handlers request-handler]
  (run-pipeline response-handlers
    read-channel
    (fn [[request response ch timeout]]
      (run-pipeline (read-channel ch (timeout))
	:error-handler (fn [_ ex]
			 (if-not (zero? (timeout))
			   (siphon (request-handler request) response)
			   (enqueue (:error response) [request ex]))
			 (complete
			   (when-not (closed? ch)
			     #(read-channel ch))))
	#(do
	   (if (nil? %)
	     (throw (Exception. "Request connection severed."))
	     (enqueue (:success response) %))
	   nil)))
    #(when % (%))
    (fn [_] (restart))))

(defn pipelined-request-handler [connection-generator]
  (let [handlers (channel)
	request-handler (fn this
			  ([request]
			     (this request -1))
			  ([request timeout]
			     (let [response (pipeline-channel)]
			       (run-pipeline (connection-generator)
				 (fn [ch]
				   (enqueue ch request)
				   (enqueue handlers [request response ch (timeout-fn timeout)])
				   response)))))]
    (start-pipelined-request-handler handlers request-handler)
    request-handler))



