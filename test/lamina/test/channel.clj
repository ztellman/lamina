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
    [clojure.walk]
    [clojure.contrib.combinatorics]))

;;

(defn async-enqueue [ch s slow?]
  (future
    (try
      (doseq [x s]
	(enqueue ch x)
	(if slow?
	  (Thread/sleep 1)
	  (Thread/sleep 0 1)))
      (close ch)
      (catch Exception e
	(.printStackTrace e)))))

(declare callback)

(defmacro output-of [f & body]
  `(let [coll# (atom [])]
     (binding [callback (fn [msg#] (swap! coll# conj (~f msg#)))]
       ~@body
       @coll#)))

;;;

(defmacro close-output= [expected ch & body]
  (let [ch-sym (gensym "ch")
	body (postwalk-replace {'ch ch-sym} body)]
    (let [f (fn [ch]
	      (fn [msg]
		[msg (closed? ch)]))]
      `(do
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym) (receive-all ~ch-sym callback) ~@body))))
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym) (receive-in-order ~ch-sym callback) ~@body))))
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym)
			      (receive ~ch-sym (fn this# [msg#]
						 (callback msg#)
						 (when-not (closed? ~ch-sym)
						   (receive ~ch-sym this#))))
			      ~@body))))))))

(deftest test-close
  (close-output= [[1 false] [2 true]]
    (sealed-channel 1 2))
  (close-output= [[1 false] [2 false] [nil true]]
    (channel 1 2)
    (close ch))
  (close-output= [[1 false] [2 false] [nil true]]
    (channel)
    (enqueue-and-close ch 1 2)))
;;;

;; Register a series of listeners that only receive one value
(deftest test-simple-listen
  (let [ch (channel)
	coll (atom [])
	num 1e3]
    (async-enqueue ch (range num) true)
    (dotimes [_ num]
      (let [latch (promise)]
	(listen ch (fn [msg]
		     [false (fn [msg]
			      (swap! coll conj msg)
			      (deliver latch nil))]))
	@latch))
    (is (= (range num) @coll))))

;; Register large number of listeners, but only let one receive each value
(deftest test-listen
  (let [ch (channel)
	coll (atom [])
	waiting-for (ref 0)
	num 1e3
	latch (promise)]
    (async-enqueue ch (range num) true)
    (while (< (count @coll) num)
      (listen ch (fn [msg]
		   (when (= msg (ensure waiting-for))
		     (alter waiting-for inc)
		     [false (fn [msg]
			      (swap! coll conj msg)
			      (when (= (dec num) msg)
				(deliver latch nil)))]))))
    @latch
    (is (= (range num) @coll))))


;; polling

(deftest test-simple-poll
  (let [ch (channel)
	num 1e3]
    (let [coll (atom [])]
      (async-enqueue ch (range num) false)
      (dotimes [i num]
	(when-let [[ch msg] (wait-for-message (poll {:ch ch}))]
	  (swap! coll conj msg)))
      (is (= (range num) @coll)))))

(deftest test-poll
  (let [u (channel)
	v (channel)
	num 1e3]
    (let [colls {:u (atom [])
		 :v (atom [])}]
      (async-enqueue u (range num) false)
      (async-enqueue v (range num) false)
      (dotimes [i (+ 2 (* 2 num))]
	(when-let [[ch msg] (wait-for-message (poll {:u u, :v v}))]
	  (swap! (colls ch) conj msg)))
      (is (= (concat (range num) [nil]) @(:u colls)))
      (is (= (concat (range num) [nil]) @(:v colls))))))

(deftest test-poll-timeout
  (let [ch (channel)]
    (is (= nil (wait-for-message (poll {:ch ch} 0) 0)))))

;; synchronous methods

(deftest test-wait-for-message
  (let [num 1e2]
    (let [ch (channel)]
      (async-enqueue ch (range num) false)
      (dotimes [i num]
	(is (= i (wait-for-message ch 100)))))))

(deftest test-channel-seq
  (let [ch (sealed-channel 1 nil)]
    (is (= [1] (channel-seq ch))))

  (let [in (range 1e3)
	target (last in)
	ch (channel)]
    (async-enqueue ch in false)
    (is (= in
	   (loop [out []]
	     (Thread/sleep 0 1)
	     (let [n (channel-seq ch)]
	       (if-not (= target (last n))
		 (recur (concat out n))
		 (concat out n))))))))

;; seq-like methods

(deftest test-map*
  (let [s (range 10)
	f #(* % 2)]

    (let [ch (apply sealed-channel s)]
      (= (map f s) (channel-seq (map* f ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (map f s) (channel-seq (map* f ch))))))

(deftest test-filter*
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (filter even? s) (channel-seq (filter* even? ch))))

    (let [ch (channel)]
      (async-enqueue ch s true)
      (= (filter even? s) (channel-seq (filter* even? ch))))))

(deftest test-reduce*
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (reduce + s) (wait-for-message (reduce* + ch))))

    (let [ch (channel)]
      (async-enqueue ch s false)
      (= (reduce + s) (wait-for-message (reduce* + ch) 2500)))))

(deftest test-reductions*
  (let [s (range 10)]

    (let [ch (apply sealed-channel s)]
      (= (reductions + s) (channel-seq (reductions* + ch))))

    (let [ch (channel)]
      (async-enqueue ch s false)
      (= (reductions + s) (channel-seq (reductions* + ch))))))
