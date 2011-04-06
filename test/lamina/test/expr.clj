;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.expr
  (:use
    [lamina.core]
    [clojure.test]))

(def *sleep-interval* 10)

(defmacro task* [& body]
  `(task
     (Thread/sleep 0 *sleep-interval*)
     ~@body))

(defmacro is= [expected expr]
  `(do
     (binding [*sleep-interval* 50]
       (dotimes [_# 1]
	 (is (= ~expected (wait-for-result (async ~expr) 2000)) (str "interval=" *sleep-interval*))))
     (binding [*sleep-interval* 1]
       (dotimes [_# 1]
	 (is (= ~expected (wait-for-result (async ~expr) 2000)) (str "interval=" *sleep-interval*))))))

(defmacro ch-is= [expected ch s expr]
  `(is
     (= ~expected
       (wait-for-result
	 (let [~ch (channel)
	       result# (async ~expr)]
	   (future
	     (Thread/sleep 100)
	     (doseq [x# ~s]
	       (enqueue ~ch x#)
	       (Thread/sleep *sleep-interval*)))
	   result#)
	 2000))))

(deftest test-basic-exprs
  (is= 6 (task* (+ 1 (task* (+ 2 3)))))
  (is= 6 (reduce #(task* (+ %1 %2)) [1 2 3]))
  (is= 6 (->> (range 3) (map inc) (reduce +)))
  (is= 6 (->> (range 3) (map #(task* (+ 1 %))) (reduce #(task* (+ %1 %2))))))

(deftest test-exceptions
  (is= 3
    (try
      (throw (Exception.))
      (catch Exception e
	(task* 3))))
  (is= 4
    (try
      (throw (Exception.))
      (catch RuntimeException e
	(task* 3))
      (catch Exception e
	(task* 4))))
  (is= 5
    (try
      (task* (+ 1 2))
      (finally
	(task* (+ 2 3))))))

(deftest test-fns
  (is= 3
    ((fn [[x]] x) [3]))
  (is= 3
    ((fn ([[x]] x)) [3]))
  (is= 3
    ((fn abc [[x]] x) [3]))
  (is= 3
    ((fn abc ([[x]] x)) [3])))

'(deftest test-force
  (is= [1 2 3 4]
    [(force (task* 1))
     (force (task* 2))
     (force (task* 3))
     (force (task* 4))])
  (is= [1 2 3 4]
    (concat
      (force-all [(task* 1) (task* 2)])
      (force-all [(task* 3) (task* 4)])))
  (is= [1 4 10]
    (do
      (force
	(when (task* (+ 1 2))
	  (task* (+ 3 4))))
      (let [a (concat [1 2] [3 4])
	    b (force (task* (last a)))
	    c (task* (+ b 6))]
	[1 b c]))))

'(deftest test-channels
  (is= [1 2]
    (let [ch (channel 1 2)]
      [(read-channel ch) (read-channel ch)]))
  (ch-is= [1 1]
    ch [1 2]
    [(read-channel* ch) (read-channel* ch)])
  (ch-is= [1 2]
    ch [1 2]
    (concat (force [(read-channel* ch)]) (force [(read-channel* ch)])))
  (ch-is= [1 2]
    ch [1 2]
    (concat (force-all [(read-channel* ch)]) [(read-channel ch)]))
  (ch-is= [1 2]
    ch [1 2]
    [(read-channel ch) (read-channel ch)])
  (ch-is= [1 2 3]
    ch [1 2 3]
    [(read-channel ch)
     (read-channel ch)
     (read-channel ch)])
  (is= 3
    (let [ch (channel 1 2 3)
	  a (read-channel ch)
	  b (+ 1 (read-channel ch))]
      b))
  (is= [1 2 3]
    (let [ch (closed-channel 1 2 3)]
      (loop [accum []]
	(if (drained? ch)
	  accum
	  (recur (conj accum (read-channel ch))))))))

(deftest test-task
  (is= [1 2 3]
    (let [[a b c] [(task* 1) (task* 2) (task* 3)]]
      [a b c]))
  (is= (range 100)
    ((fn this [x]
       (if (zero? x)
	 [0]
	 (task* (conj (this (dec x)) x))))
     99)))

(deftest test-recur
  (is= [0 1 2]
    (for [x (range 3)] (task* x)))
  (is= (range 100)
    ((fn [x]
       (if (= 100 (count x))
	 x
	 (recur (task* (conj x (count x))))))
     []))
  (is= 4
    ((fn
       ([x y] (recur (task* (+ x y))))
       ([x] (task* (inc x))))
     1 2)))

(deftest test-lazy-seq
  (is= [0 1 2]
    ((fn this [x]
       (lazy-seq
	 (if (zero? x)
	   [x]
	   (task* (concat (this (dec x)) [x])))))
     2)))

