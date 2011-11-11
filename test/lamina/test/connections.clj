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
    [lamina core connections trace]
    [lamina.core.pipeline :only (success-result error-result)])
  (:require
   [clojure.contrib.logging :as log])
  (:import java.util.concurrent.TimeoutException))

;;;

(def probes
  {:connection:lost log-info
   :connection:opened log-info
   :connection:failed log-info})

(defn simple-echo-server []
  (let [[a b] (channel-pair)]
    (run-pipeline
      (receive-in-order b #(enqueue b %))
      (fn [_] (do (close a) (close b))))
    [a #(do (close a) (close b))]))

(defn alternating-delay-echo-server []
  (let [[a b] (channel-pair)]
    (run-pipeline b
      :error-handler (fn [_] (close a) (close b))
      read-channel
      (wait-stage 100)
      #(when-not (drained? b)
	 (enqueue b %))
      (constantly b)
      read-channel
      #(when-not (drained? b)
	 (enqueue b %))
      (fn [_] (restart)))
    [a #(do (close a) (close b))]))

(defn error-server []
  (let [[a b] (channel-pair)]
    (run-pipeline
      (receive-in-order b (fn [_] (enqueue b (RuntimeException. "fail!"))))
      (fn [_] (do (close a) (close b))))
    [a #(do (close a) (close b))]))

(defmacro with-server [server-fn & body]
  `(let [chs# (ref nil)
	 close-fns# (ref nil)
	 server# (ref true)
	 ~'connect (fn []
		     (dosync
		       (if-not (ensure server#)
			 (error-result nil)
			 (let [[ch# close-fn#] (~server-fn)]
			   (alter chs# conj ch#)
			   (alter close-fns# conj close-fn#)
			   (success-result ch#)))))
	 ~'start-server (fn []
			  (dosync (ref-set server# true)))
	 ~'stop-server (fn []
			 (doseq [f# (dosync
				      (ref-set server# false)
				      (let [fns# (ensure close-fns#)]
					(ref-set close-fns# nil)
					(ref-set chs# nil)
					fns#))]
			   (f#)))]
     ~@body))

;;;

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
  (simple-response pipelined-client)
  (simple-response (fn [& args] (client-pool 1 #(apply client args)))))

(deftest timeouts-can-be-used
  (simple-response client 1000)
  (simple-response pipelined-client 1000)
  (simple-response (fn [& args] (client-pool 1 #(apply client args)))))
 
(defn dropped-connection [client-fn]
  (with-server simple-echo-server
    (let [f (client-fn #(connect) {:description "dropped-connection"})]
      (try
	(stop-server)
	(let [results (doall (map #(f %) (range 10)))]
	  (Thread/sleep 100)
	  (start-server)
	  (Thread/sleep 1000)
	  (is (= (range 10) (map #(wait-for-result % 10000) results))))
	(finally
	  (close-connection f))))))

(deftest test-dropped-connection
  (dropped-connection client)
  (dropped-connection pipelined-client)
  (dropped-connection (fn [& args] (client-pool 1 #(apply client args)))))

(defn works-after-a-timed-out-request [client-fn initially-disconnected]
  (with-server alternating-delay-echo-server
    (when initially-disconnected
      (stop-server))
    (let [f (client-fn #(connect) {:probes (comment probes) :description "dropped-and-restored"})]
      (when-not initially-disconnected
        (stop-server))
      (try
        (is (thrown? TimeoutException @(f "echo" 10)))
        (start-server)
	(is (thrown? TimeoutException @(f "echo2" 10)))
        (is (= "echo3" @(f "echo3" 10000)))
        (finally
	  (close-connection f))))))

(defn persistent-connection-stress-test [client-fn]
  (with-server simple-echo-server
    (let [continue (atom true)
	  f (client-fn #(connect) {:probes (comment probes) :description "stress-test"})]
      ; periodically drop
      (.start
	(Thread.
	  #(loop []
	     (Thread/sleep 4000)
	     (stop-server)
             ;;(println "Disconnecting")
	     (Thread/sleep 100)
	     (start-server)
             ;;(println "Reconnecting")
	     (when @continue
	       (recur)))))
      (try
        (let [s (range 1e4)]
	  (is (= s
                (->> s
                  (map (fn [x]
                         (run-pipeline
                           (f x 1e5)
                           :error-handler (fn [_])
                           #(do 
                              #_(when (zero? (rem % 100))
                                (println %))
                              %))))
                  doall
                  (map deref)
                  ))))
        (finally
	  (reset! continue false)
	  (close-connection f))))))
    
(deftest test-keeps-on-working-after-a-timed-out-request
  (testing "with the connection initially disconnected"
    (works-after-a-timed-out-request pipelined-client true)
    (works-after-a-timed-out-request client true))
  (testing "with the connection disconnected afterwards"
    (works-after-a-timed-out-request pipelined-client false)
    (works-after-a-timed-out-request client false)
    ))

(deftest test-keeps-working-despite-constant-disconnects
  (persistent-connection-stress-test client)
  (persistent-connection-stress-test pipelined-client))

(defn errors-propagate [client-fn]
  (with-server error-server
    (start-server)
    (let [f (client-fn #(connect) {:description "error-server"})]
      (is (thrown? RuntimeException @(f "fail?" 100)))
      (is (thrown? RuntimeException @(f "fail?" 100))))))

(deftest test-error-propagation
  (errors-propagate client)
  (errors-propagate pipelined-client)
  (errors-propagate (fn [& args] (client-pool 1 #(apply client args)))))

;;;;

(def exception (Exception. "boom"))

(defmacro with-handler [handler options & body]
  `(do
     (let [[a# b#] (channel-pair)
	   ~'ch a#
	   server# (server b# ~handler ~options)]
       ~@body)
     (let [[a# b#] (channel-pair)
	   ~'ch a#
	   server# (pipelined-server b# ~handler ~options)]
       ~@body)))

(deftest test-server-error-handler
  (with-handler (fn [_ _] (throw exception)) nil
    (enqueue ch 1 2)
    (is (= [exception exception] (channel-seq ch))))

  (with-handler (fn [_ _] (run-pipeline nil :error-handler (fn [_]) (fn [_] (throw exception)))) nil
    (enqueue ch 1 2)
    (is (= [exception exception] (channel-seq ch))))

  (with-handler (fn [_ _] (throw exception)) {:include-request true}
    (enqueue ch 1 2)
    (is (= [{:request 1, :response exception} {:request 2, :response exception}] (channel-seq ch))))
  
  (with-handler (fn [_ _] (run-pipeline nil :error-handler (fn [_]) (fn [_] (throw exception)))) {:include-request true}
    (enqueue ch 1 2)
    (is (= [{:request 1, :response exception} {:request 2, :response exception}] (channel-seq ch)))))

(deftest test-server-handler
  (with-handler (fn [ch req] (enqueue ch req)) nil
    (enqueue ch 1 2)
    (is (= [1 2] (channel-seq ch))))
  (with-handler (fn [ch req] (enqueue ch req)) {:include-request true}
    (enqueue ch 1 2)
    (is (= [{:request 1, :response 1} {:request 2, :response 2}] (channel-seq ch)))))

(deftest test-server-probes
  (let [calls (channel)
	results (channel)
	errors (channel)]
    (with-handler (fn [ch req] (enqueue ch req)) {:probes {:calls calls, :results results}}
      (enqueue ch 1 2)
      (let [calls (channel-seq calls)
	    results (channel-seq results)]
	(is (= [[1] [2]] calls))
	(is (= [[1] [2]] (map :args results)))))
    (with-handler (fn [ch req] (throw exception)) {:probes {:calls calls, :errors errors}}
      (enqueue ch 1 2)
      (let [calls (channel-seq calls)
	    errors (channel-seq errors)]
	(is (= [[1] [2]] calls))
	(is (= [[1] [2]] (map :args errors)))))))

;;;

