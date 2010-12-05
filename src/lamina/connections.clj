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

(defn- retry-connect [delay latch]
  (fn [_ _]
    (when @latch
      (swap! delay #(if (zero? %) 500 (min 64000 (* % 2))))
      (log/info (str "Waiting " @delay "ms before attempting reconnection."))
      (restart))))

(defn- handle-connection [delay connection-lost-callback connection]
  (fn [ch]
    (let [[a b] (fork 2 ch)
	  connection @connection
	  close-signal (constant-channel)]
      (reset! delay 0)
      (enqueue (:success connection) (splice a ch))
      (receive-all b
	(fn [_]
	  (when (closed? b)
	    (if connection-lost-callback
	      (connection-lost-callback)
	      (log/warn "Connection dropped."))
	    (enqueue close-signal nil)
	    (enqueue (:error connection)
	      [b (Exception. "Connection severed.")]))))
      (read-channel close-signal))))

(defn- connect-loop [connection-generator connection-lost-callback connection]
  (let [latch (atom true)
	delay (atom 0)]
    (run-pipeline nil
      (fn [_]
	(if @latch
	  (do (reset! connection (pipeline-channel)) nil)
	  (complete nil)))
      (pipeline :error-handler (retry-connect delay latch)
	(fn [_] (read-channel (wait-channel @delay)))
	(fn [_] (connection-generator)))
      (handle-connection delay connection-lost-callback connection)
      restart)
    (fn []
      (reset! latch false)
      (run-pipeline @connection
	(fn [ch] (enqueue-and-close nil))))))

(defn persistent-connection
  "Given a function that generates a connection (a pipeline-channel that yields a channel),
   returns a function that, given zero parameters, returns a connection.

   Behind the scenes, this will maintain a single live connection, reconnecting when
   necessary.  If 'connection-lost-callback' is specified, it will be called every time
   the connection drops.

   To close the connection, pass 'true' into the returned function."
  ([connection-generator]
     (persistent-connection connection-generator nil))
  ([connection-generator connection-lost-callback]
     (let [connection (atom (pipeline-channel
			      nil-channel
			      (constant-channel [nil nil])))
	   stop-loop (connect-loop
		       connection-generator
		       connection-lost-callback
		       connection)]
       
       (fn
	 ([]
	    (run-pipeline @connection 
	      :error-handler (fn [_ _] (restart))))
	 ([close?]
	    (when close?
	      (stop-loop)))))))

;;

(defn- timeout-fn [timeout]
  (if (neg? timeout)
    (constantly timeout)
    (let [now (System/currentTimeMillis)]
      #(max 0 (- (+ now timeout) (System/currentTimeMillis))))))

(defn- siphon-pipeline-channel [src dst]
  (doseq [ch [:success :error]]
    (receive (ch src) #(enqueue (ch dst) %))))

(defn- response-handler [ch response]
  (fn [msg]
    (cond
      (and (nil? msg) (closed? ch))
      (throw (Exception. "request connection severed"))
      
      (instance? Throwable msg)
      (enqueue (:error response) msg)
      
      :else
      (enqueue (:success response) msg))
    nil))

(defn- start-client-loop [connection-generator requests]
  (run-pipeline requests
    read-channel
    (fn [[request response timeout]]
      (run-pipeline (connection-generator)
	:error-handler (fn [_ ex]
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
	    (response-handler ch response)))
	#(when % (%))))
    (fn [_] (restart))))

(defn client
  "Given a function that returns a connection, returns a function that takes a
   request value and optionally a timeout, and returns a pipeline-channel
   representing the response.  This will keep a single live connection to the
   server, reconnecting when necessary.

   A new request will only be sent once the response to the previous one has
   been received."
  [connection-generator]
  (let [connection-generator (persistent-connection connection-generator)
	requests (channel)]
    (start-client-loop connection-generator requests)
    (fn this
      ([request]
	 (this request -1))
      ([request timeout]
	 (let [response (pipeline-channel)]
	   (enqueue requests [request response (timeout-fn timeout)])
	   response)))))

;;

(defn- start-pipelined-client-loop [response-handlers request-handler]
  (run-pipeline response-handlers
    read-channel
    (fn [[request response ch timeout]]
      (run-pipeline (read-channel ch (timeout))
	:error-handler (fn [_ ex]
			 (if-not (zero? (timeout))
			   (siphon-pipeline-channel (request-handler request) response)
			   (enqueue (:error response) [request ex]))
			 (complete
			   (when-not (closed? ch)
			     #(read-channel ch))))
	(response-handler ch response)))
    #(when % (%))
    (fn [_] (restart))))

(defn pipelined-client
  "Given a function that returns a connection, returns a function that takes a
   request value and optionally a timeout, and returns a pipeline-channel
   representing the response.  This will keep a single live connection to the
   server, reconnecting when necessary.

   Requests will be sent as soon as they're made, with the assumption that
   responses will be returned in the same order."
  [connection-generator]
  (let [handlers (channel)
	connection-generator (persistent-connection connection-generator)
	request-handler (fn this
			  ([request]
			     (this request -1))
			  ([request timeout]
			     (let [response (pipeline-channel)]
			       (run-pipeline (connection-generator)
				 (fn [ch]
				   (enqueue ch request)
				   (enqueue handlers [request response ch (timeout-fn timeout)])))
			       response)))]
    (start-pipelined-client-loop handlers request-handler)
    request-handler))

;;

(defn persistent-listener
  [connection-generator connection-primer]
  (let [listen-channel (channel)
	connection-generator (persistent-connection connection-generator)]
    (run-pipeline nil
      (fn [_]
	(connection-generator))
      (fn [ch]
	(connection-primer ch)
	(siphon ch listen-channel)
	(let [ch (fork ch)
	      close-signal (constant-channel)]
	  (receive-all ch #(when (closed? ch) (enqueue close-signal nil)))
	  (read-channel close-signal))
	restart))
    listen-channel))

;;

(defn server [ch handler]
  (run-pipeline ch
    read-channel
    #(let [c (constant-channel)]
       (handler c %)
       (read-channel c))
    #(enqueue ch %)
    (fn [_] (restart)))
  (fn []
    (enqueue-and-close ch)))

(defn pipelined-server [ch handler]
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
      (fn [_] (restart))))
  (fn []
    (enqueue-and-close nil)))
