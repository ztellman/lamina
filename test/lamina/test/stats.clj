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
    [lamina stats core]
    [lamina.test utils])
  (:require
    [lamina.time :as t]))

;;;

(defn run-stats-test [f values period window]
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (f
              {:period period
               :window window
               :task-queue q}
              ch)]
    (doseq [vs values]
      (when-not (empty? vs)
        (apply enqueue ch vs))
      (t/advance q))
    (channel->seq ch*)))

;;;

(deftest test-average
  (are [expected-ranges inputs window]
    (every?
      (fn [[[low high] value]]
        (<= low value high))
      (map list
        expected-ranges
        (run-stats-test moving-average inputs (t/seconds 1) window)))

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

    [55 5050 2] [(range 11) (range 101) [2]]

    ;; todo: more tests?
    
    ))

(deftest test-quantiles
  )



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
