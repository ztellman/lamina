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

(defn sub [o]
  (subscribe o {:a (observer #(prn %))}))

(deftest test-observable
  (let [o (observable)]
    (is (sub o))
    (is (= [1] (read-string (with-out-str (message o [1])))))
    (is (unsubscribe o [:a]))
    (is (=  (with-out-str (message o [1]))))
    (is (close o))
    (is (not (sub o)))))

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
  (is (= [1 2 3] (siphon-output #(siphon %1 %2) [1 2 3])))
  (is (= [2 4] (siphon-output #(siphon-while even? %1 %2) [2] [4 5])))
  (is (= [2 4 6] (siphon-output #(siphon (partial * 2) %1 %2) [1 2 3])))
  (is (= [1 3] (siphon-output #(siphon-when odd? %1 %2) [1 2 3 4]))))
