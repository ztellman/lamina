;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.queue
  (:use
    [lamina.core.queue]
    [clojure.test])
  (:require
    [lamina.core.observable :as o]))

(declare ^{:dynamic true} accumulator)

(defmacro output= [value & body]
  `(binding [accumulator (atom [])]
     (do ~@body)
     (is (= ~value @accumulator))))

(defmacro output-set= [value & body]
  `(binding [accumulator (atom [])]
     (do ~@body)
     (is (= (set ~value) (set @accumulator)))))

(deftest test-receive
  (let [f (fn [] #(swap! accumulator conj %))
	o (o/observable)
	q (queue o)]
    (output= [1 1 1 1 1 1 2 3 3 4 4 4]
      (receive q (f) (f) (f))
      (receive q (f))
      (receive q (f) (f))
      (o/message o [1 2 3 4])
      (receive q (f))
      (receive q (f) (f))
      (receive q (f) (f) (f)))))

(deftest test-listen
  (let [f (fn [id cnt]
	    (let [c (ref cnt)]
	      (fn [msg]
		(when (<= 0 (alter c dec))
		  [true
		   (fn [msg]
		     (swap! accumulator conj [id msg]))]))))
	o (o/observable)
	q (queue o)]
    (output-set= [[:a 1] [:a 2]
		  [:b 2] [:b 3]
		  [:c 1] [:c 2] [:c 3]]
      (o/message o [1])
      (listen q (f :a 2) (f :c 3))
      (listen q (f :b 2))
      (o/message o [2 3 4])))

  (let [f (fn [id cnt]
	    (let [c (ref cnt)]
	      (fn [msg]
		(when (<= 0 (alter c dec))
		  [true (fn [msg] (swap! accumulator conj [id msg]))]))))
	o (o/observable)
	q (queue o)]
    (output-set= [[:a 1] [:a 2]
		  [:b 2] [:b 3]
		  [:c 1] [:c 2] [:c 3]]
      (o/message o [1])
      (listen q (f :a 2) (f :c 3))
      (listen q (f :b 2))
      (o/message o [2 3 4]))))

(deftest test-cancel-callback
  (let [f (fn [] #(swap! accumulator conj %))
	o (o/observable)
	q (queue o)
	[a b] [(f) (f)]]
    (output= [1]
      (receive q a b)
      (cancel-callbacks q [a])
      (o/message o [1]))))

(defn queue-seq [q]
  (take-while
    #(not= % ::none)
    (repeatedly #(dequeue q ::none))))

(deftest test-copy-queue
  (let [o (o/observable)
	q (queue o [1])]
    (= [1] (queue-seq q))
    (is (= [] (queue-seq q)))
    (let [q* (copy-queue q)]
      (o/message o [1 2])
      (is (= [1 2] (queue-seq q)))
      (o/message o [3 4])
      (is (= [3 4] (queue-seq q)))
      (is (= [1 2 3 4] (queue-seq q*)))
      (let [q** (copy-and-alter-queue q* #(map (partial * 2) %))]
	(o/message o [1 2])
	(is (= [1 2] (queue-seq q)))
	(is (= [1 2] (queue-seq q*)))
	(is (= [2 4] (queue-seq q**)))))))


