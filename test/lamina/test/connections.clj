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
   [clojure.contrib.logging :as log])
  (:import java.util.concurrent.TimeoutException))

(defn simple-echo-server []
  (let [[a b] (channel-pair)]
    (run-pipeline
      (receive-in-order b #(enqueue b %))
      (fn [_] (do (close a) (close b))))
    [a #(do (close a) (close b))]))

(defn error-server []
  (let [[a b] (channel-pair)]
    (run-pipeline
      (receive-in-order b (fn [_] (enqueue b (Exception. "fail!"))))
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
    (let [connection (persistent-connection #(connect) {:description "test-connection"})]
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
        (let [f (client-fn #(connect) {:description "simple-response"})]
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
  (simple-response client 1000)
  (simple-response pipelined-client 1000))

(defn dropped-connection [client-fn]
  (with-server simple-echo-server
    (let [f (client-fn #(connect) {:description "dropped-connection"})]
      (try
	(stop-server)
	(let [results (doall (map #(f %) (range 10)))]
	  (Thread/sleep 100)
	  (start-server)
	  (Thread/sleep 1000)
	  (is (= (range 10) (map #(wait-for-result % 100) results))))
	(finally
	  (close-connection f))))))

(deftest test-dropped-connection
  (dropped-connection client)
  (dropped-connection pipelined-client))

(defn works-after-a-timed-out-request [client-fn initially-disconnected]
  (with-server simple-echo-server
    (when initially-disconnected
      (stop-server))
    (let [f (client-fn #(connect) {:description "dropped-and-restored"})]
      (when-not initially-disconnected
        (stop-server))
      (try
        (is (thrown? TimeoutException (wait-for-result (f "echo" 100))))
        (start-server)
	;; big timeout to ensure the persistent connection catches up
        (is (= "echo2" (wait-for-result (f "echo2" 4000))))
        (finally
	  (close-connection f))))))

(deftest test-keeps-on-working-after-a-timed-out-request
  (testing "with the connection initially disconnected"
    (works-after-a-timed-out-request pipelined-client true)
    (works-after-a-timed-out-request client true))
  (testing "with the connection disconnected afterwards"
    (works-after-a-timed-out-request pipelined-client false)
    (works-after-a-timed-out-request client false)))

(defn errors-propagate [client-fn]
  (with-server error-server
    (start-server)
    (let [f (client-fn #(connect) {:description "error-server"})]
      (is (thrown? Exception @(f "fail?"))))))

(deftest test-error-propagation
  (errors-propagate client)
  (errors-propagate pipelined-client))
