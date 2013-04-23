;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.stats
  (:use
    [clojure test]
    [lamina stats core trace query]
    [lamina.test utils])
  (:require
    [lamina.time :as t]))

;;;

(defn run-stats-test [f values period options]
  (->> (query-seq
         #(f options %)
         {:period period
          :timestamp first
          :payload second}
         (->> values
           (map vector (iterate (partial + (inc period)) 0))
           (map (fn [[t vs]] (map vector (repeat t) vs)))
           (apply concat)))
    (map :value)))

;;;

(deftest test-average
  (are [expected-ranges inputs window]
    (every?
      (fn [[[low high] value]]
        (<= low value high))
      (map list
        expected-ranges
        (run-stats-test moving-average inputs (t/seconds 1) {:window window})))

    [[4 5] [10 11] [10 15]] [(range 10) (range 10 20) [15]] (t/seconds 1)
    [[4 5] [10 11] [10 15]] [(range 10) (range 10 20) (range 14 16)] (t/seconds 1)

    ;; todo: more tests?
    
    ))

(deftest test-variance
  (are [expected-ranges inputs]
    (every?
      (fn [[[low high] value]]
        (<= low (Math/sqrt value) high))
      (map list
        expected-ranges
        (run-stats-test variance inputs (t/seconds 1) nil)))

    [[25 35] [250 350]] [(range 100) (range 1000)]

    ;; todo: more tests?
    
    ))

(deftest test-sum
  (are [expected-values inputs]
    (= expected-values (run-stats-test sum inputs (t/seconds 1) nil))

    [55.0 5050.0 2.0] [(range 11) (range 101) [2]]

    ;; todo: more tests?
    
    ))

(deftest test-quantiles
  (are [expected-values inputs qnt]
    (= expected-values
      (run-stats-test moving-quantiles inputs (t/seconds 1) {:quantiles qnt}))

    [{0.5 49.5}] [(range 100)] [0.5])

  (are [expected-values inputs qnt]
    (= expected-values
      (run-stats-test quantiles inputs (t/seconds 1) {:quantiles qnt}))

    [{0.5 49.5}] [(range 100)] [0.5]))

(deftest test-sample
  #_(prn
    (last
      (run-stats-test moving-sample (map vector (range 1e6)) (t/seconds 1)
        {:sample-size 20
         :window (t/seconds 1e4)})))
  #_(prn
    (last
      (run-stats-test sample (map vector (range 1e4)) (t/seconds 1)
        {:sample-size 20}))))


;;;

(deftest ^:benchmark benchmark-sum
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (sum {:task-queue q} ch)]
    (bench "sum"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-rate
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (rate {:task-queue q} ch)]
    (bench "rate"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-moving-average
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (moving-average {:task-queue q} ch)]
    (bench "moving-average"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-variance
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (variance {:task-queue q} ch)]
    (bench "variance"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-moving-quantiles
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (moving-quantiles {:task-queue q} ch)]
    (bench "moving-quantiles"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-sample
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (sample {:task-queue q} ch)]
    (bench "sample"
      (enqueue ch 1))))

(deftest ^:benchmark benchmark-moving-sample
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (moving-sample {:task-queue q} ch)]
    (bench "moving-sample"
      (enqueue ch 1))))
