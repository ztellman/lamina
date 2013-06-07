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
    [lamina core query]
    lamina.query.parse)
  (:require
    [lamina.time :as t]))

(deftest test-query-seq

  ;; smaller period than interval of data
  (let [val (query-seq
              #(partition-every {:period 10} %)
              {:timestamp identity}
              (range 20))]
    (is (= [10 20 30]
          (map :timestamp val)))
    (is (= [(range 10) (range 10 20) nil]
          (map :value val))))

  ;; larger period than interval of data
  (let [val (query-seq
              #(partition-every {:period 100} %)
              {:timestamp identity}
              (range 20))]
    (is (= [100] (map :timestamp val)))
    (is (= [(range 20)] (map :value val))))

  ;; both at once
  (let [s (range 100 120)
        f1 ".partition-every(period: 10ms)"
        f2 ".partition-every(period: 100ms)"
        val (query-seqs
              {f1 s
               f2 s}
              {:timestamp identity})
        val1 (get val f1)
        val2 (get val f2)]

    (is (= [110 120 130]
          (map :timestamp val1)))
    (is (= [(range 100 110) (range 110 120) nil]
          (map :value val1)))

    (is (= [200] (map :timestamp val2)))
    (is (= [(range 100 120)] (map :value val2))))

  (let [s (range 100 120)
        f1 "&abc.partition-every(period: 10ms)"
        f2 "&def.partition-every(period: 15ms)"
        val (query-seqs
              {f1 nil
               f2 nil}
              {:timestamp identity
               :seq-generator (fn [pattern]
                                (assert (#{"abc" "def"} pattern))
                                s)})
        val1 (get val f1)
        val2 (get val f2)]

    (is (= [110 120 130]
          (map :timestamp val1)))
    (is (= [(range 100 110) (range 110 120) nil]
          (map :value val1)))

    (is (= [115 130] (map :timestamp val2)))
    (is (= [(range 100 115) (range 115 120)] (map :value val2)))))

(deftest test-query-stream

  ;; smaller period than interval of data
  (let [ch (query-stream
             #(partition-every {:period 10} %)
             {:timestamp identity}
             (apply closed-channel (range 20)))]
    (is (= [(range 10) (range 10 20)]
          (channel->lazy-seq ch))))

  ;; larger period than interval of data
  (let [ch (query-stream
             #(partition-every {:period 100} %)
             {:timestamp identity}
             (apply closed-channel (range 20)))]
    (is (= [(range 20)] (channel->lazy-seq ch))))

  ;; both at once
  (let [s (range 100 120)
        f1 ".partition-every(period: 10ms)"
        f2 ".partition-every(period: 100ms)"
        val (query-streams
              {f1 (apply closed-channel s)
               f2 (apply closed-channel s)}
              {:timestamp identity})
        ch1 (get val f1)
        ch2 (get val f2)]

    (is (= [(range 100 110) (range 110 120)]
          (channel->seq ch1)))
    (is (= [(range 100 120)]
          (channel->seq ch2))))

  (let [s (range 100 120)
        f1 "&abc.partition-every(period: 10ms)"
        f2 "&def.partition-every(period: 15ms)"
        val (query-streams
              {f1 nil
               f2 nil}
              {:timestamp identity
               :stream-generator (fn [pattern]
                                   (assert (#{"abc" "def"} pattern))
                                   (apply closed-channel s))})
        ch1 (get val f1)
        ch2 (get val f2)]

    (is (= [(range 100 110) (range 110 120)]
          (channel->seq ch1)))
    (is (= [(range 100 115) (range 115 120)]
          (channel->seq ch2)))))

(deftest test-group-by
  (let [val (query-seq
              ".group-by(facet).value.rate()"
              {:timestamp :value
               :period 100}
              (map #(hash-map :facet :foo, :value %) (range 20)))]

    (is (= [{:timestamp 100, :value {:foo 20}}] val))))

(deftest test-collapse
  (let [val (query-seq
             ".group-by(facet).value.collapse().foo"
             {:timestamp :value
              :period 100}
             (map #(hash-map :facet :foo, :value %) (range 20)))]
    (is (= [{:timestamp 100, :value (range 20)}] val))))

(deftest test-nested-collapsing
  (is (= '[(group-by :x [(group-by :y [:foo])
                         :bar])
           :baz]
         (parse-string-query ".group-by(x).group-by(y).foo.collapse().bar.collapse().baz"))))
