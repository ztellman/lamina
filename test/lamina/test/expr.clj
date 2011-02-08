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

(defmacro is= [expected expr]
  `(is (= ~expected @(async ~expr))))

(deftest test-basic-exprs
  (is= 6 (+ 1 (+ 2 3)))
  (is= 6 (reduce + [1 2 3]))
  (is= 6 (->> (range 3) (map inc) (reduce +)))
  (is= 6 (->> (range 3) (map #(+ 1 %)) (reduce #(+ %1 %2)))))

(deftest test-exceptions
  (is= 3 (try
	   (throw (Exception.))
	   (catch Exception e
	     3)))
  (is= 4 (try
	   (throw (Exception.))
	   (catch RuntimeException e
	     3)
	   (catch Exception e
	     4)))
  (is= 5 (try
	   (+ 1 2)
	   (finally
	     (+ 2 3)))))

'(deftest test-channels
  (is= [1 2 3]
    (let [ch (channel 1 2 3)]
      (repeatedly (fn [] 1)))))
