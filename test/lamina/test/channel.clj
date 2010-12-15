;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.channel
  (:use
    [lamina.core]
    [lamina.core.channel :only (listen)])
  (:use
    [clojure.test]
    [clojure.contrib.def]
    [clojure.contrib.combinatorics]))

;;

(defn async-enqueue [ch s slow?]
  (future
    (doseq [x s]
      (enqueue ch x)
      (if slow?
	(Thread/sleep 1)
	(Thread/sleep 0 1)))
    (enqueue-and-close ch nil)))

;; polling

(deftest test-poll
  (println "test-poll")
  (let [u (channel)
	v (channel)]
    (let [colls {:u (atom [])
		 :v (atom [])}]
      (async-enqueue u (range 100) true)
      (async-enqueue v (range 100) true)
      (doseq [i (range 200)]
	(let [[ch msg] (wait-for-message (poll {:u u, :v v}))]
	  (swap! (colls ch) conj msg)))
      (is (= (range 100) @(:u colls)))
      (is (= (range 100) @(:v colls))))))

(deftest test-poll-timeout
  (println "test-poll-timeout")
  (let [ch (channel)]
    (is (= nil (wait-for-message (poll {:ch ch} 0) 0)))))

;; synchronous methods

'(deftest test-wait-for-message
  (println "test-wait-for-message")
  (let [num 1e2]
    (let [ch (channel)]
      (async-enqueue ch (range num) false)
      (dotimes [i num]
	(println "wait-for-message" i)
	(is (= i (wait-for-message ch)))))))

(deftest test-channel-seq
  (println "test-channel-seq")
  (let [ch (sealed-channel 1 nil)]
    (is (= [1] (channel-seq ch))))

  (let [in (range 1e2)
	ch (channel)]
    (async-enqueue ch in true)
    (is (= in
	   (loop [out []]
	     (Thread/sleep 0 1)
	     (let [n (channel-seq ch)]
	       (if (seq n)
		 (recur (concat out n))
		 out)))))))

;; seq-like methods

(deftest test-map*
  (println "test-map")
  (let [s (range 10)
	f #(* % 2)]

    (let [ch (apply sealed-channel s)]
      (= (map f s) (channel-seq (map* f ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (map f s) (channel-seq (map* f ch))))))

(deftest test-filter*
  (println "test-filter")
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (filter even? s) (channel-seq (filter* even? ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (filter even? s) (channel-seq (filter* even? ch))))))

(deftest test-reduce*
  (println "test-reduce")
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (reduce + s) (wait-for-message (reduce* + ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (reduce + s) (wait-for-message (reduce* + ch))))))

(deftest test-reductions*
  (println "test-reductions")
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (reductions + s) (channel-seq (reductions* + ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (reductions + s) (channel-seq (reductions* + ch))))))
