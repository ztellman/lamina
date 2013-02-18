;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.query
  (:use
    [clojure.test]
    [lamina core trace])
  (:require
    [lamina.time :as t]))

(deftest test-partition-all
  (let [val (query-seq
              #(partition-every {:period 10} %)
              (range 20)
              {:timestamp identity})]
    (is (= [10 20 30]
          (map :timestamp val)))
    (is (= [(range 11) (range 11 20) nil]
          (map :value val))))
  (let [val (query-seq
              #(partition-every {:period 100} %)
              (range 20)
              {:timestamp identity})]
    (is (= [100] (map :timestamp val)))
    (is (= [(range 20)] (map :value val)))))

(deftest test-group-by
  (let [val (query-seq
              ".group-by(facet).value.rate()"
              (map #(hash-map :facet :foo, :value %) (range 20))
              {:timestamp :value
               :period 100})]

    ;; todo: this should be 100, if we're being picky
    (is (= [{:timestamp 200, :value {:foo 20}}] val))))


