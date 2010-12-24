;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.observable
  (:use
    [clojure test walk]
    [lamina.core.observable]))

(declare accumulator)

(defn sub [key o]
  (subscribe o {key (observer #(apply swap! accumulator conj %))}))

(defmacro output= [value & body]
  `(binding [accumulator (atom [])]
     (do ~@body)
     (is (= ~value @accumulator))))

(deftest test-observable
  (let [o (observable)]
    (output= [1]
      (sub :a o)
      (message o [1]))
    (output= [2]
      (sub :a o)
      (message o [2]))
    (output= [3 3]
      (sub :b o)
      (message o [3]))
    (output= []
      (unsubscribe o [:a :b])
      (message o [4]))
    (is (= true (close o)))))

(deftest test-constant-observable
  (let [o (constant-observable)]
    (output= [1]
      (sub :a o)
      (is (= false (message o [1]))))
    (is (= false (message o [2]))))
  (let [o (constant-observable)]
    (output= [1 1]
      (is (= false (message o [1 2])))
      (sub :a o)
      (sub :b o)))
  (let [o (constant-observable)]
    (output= [1 1]
      (sub :a o)
      (sub :b o)
      (is (= false (message o [1 2]))))))

(defn siphon-output [f & input]
  (let [r (atom [])
	a (observable)
	b (observable)]
    (f a b)
    (subscribe b {:a (observer #(swap! r conj %))})
    (doseq [i input]
      (message a i))
    (apply concat @r)))

(deftest test-siphon
  (is (= [1 2 3]
	   (siphon-output #(siphon %1 {%2 identity})
	 [1 2 3])))
  (is (= [2 4 6]
	 (siphon-output #(siphon %1 {%2 (fn [msgs] (map (partial * 2) msgs))})
	 [1 2 3])))
  (is (= [1 3]
	 (siphon-output #(siphon %1 {%2 (fn [msgs] (filter odd? msgs))})
	 [1 2 3 4]))))
