;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.connections
  (:use
    [clojure test walk]
    [lamina core connections]
    [lamina.core.pipeline :only (success-result error-result)])
  (:require
    [clojure.contrib.logging :as log]))

(defn simple-echo-server []
  (let [[a b] (channel-pair)]
    (run-pipeline
      (receive-in-order b #(enqueue b %))
      (fn [_] (do (close a) (close b))))
    [a #(do (close a) (close b))]))

(defmacro with-server [server-fn & body]
  `(let [chs# (atom nil)
	 close-fns# (atom nil)
	 server# (atom true)
	 ~'connect (fn []
		     (if-not @server#
		       (error-result [nil nil])
		       (let [[ch# close-fn#] (~server-fn)]
			 (swap! chs# conj ch#)
			 (swap! close-fns# conj close-fn#)
			 (success-result ch#))))
	 ~'start-server (fn []
			  (reset! server# true))
	 ~'stop-server (fn []
			 (reset! server# false)
			 (doseq [f# @close-fns#]
			   (f#))
			 (reset! close-fns# nil)
			 (reset! chs# nil))]
     ~@body))

(deftest test-persistent-connection
  (with-server simple-echo-server
    (let [connection (persistent-connection #(connect) "test-connection")]
      (try
	(Thread/sleep 100)
	(stop-server)
	(start-server)
	(is (channel? (wait-for-result (connection) 1000)))
	(stop-server)
	(Thread/sleep 1000)
	(start-server)
	(is (channel? (wait-for-result (connection) 1000)))
	(finally
	  (close-connection connection))))))

(defn simple-response
  ([client-fn timeout]
      (with-server simple-echo-server
        (let [f (client-fn #(connect) "simple-response")]
          (try
            (dotimes [i 10]
              (is (= i (wait-for-result (f i timeout) 1000))))
            (finally
             (close-connection f))))))
  ([client-fn]
     (simple-response client-fn -1)))

(deftest test-simple-response
  (simple-response client)
  (simple-response pipelined-client))

(deftest timeouts-can-be-used
  (simple-response client 1000))

(defn dropped-connection [client-fn]
  (with-server simple-echo-server
    (let [f (client-fn #(connect) "dropped-connection")]
      (try
	(stop-server)
	(let [results (map #(f %) (range 10))]
	  (start-server)
	  (Thread/sleep 1000)
	  (is (= (range 10) (map #(wait-for-result % 100) results))))
	(finally
	  (close-connection f))))))

(deftest test-dropped-connection
  (dropped-connection client))
