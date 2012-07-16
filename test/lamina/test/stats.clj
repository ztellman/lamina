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
    [lamina.stats utils]
    [lamina.test utils])
  (:require
    [lamina.stats.moving-average :as avg]
    [lamina.stats.variance :as var]))

;;;

(deftest ^:benchmark benchmark-rate
  (let [ch (channel)]
    (rate ch)
    (bench "rate operator"
      (enqueue ch 1))
    (close ch)))

(deftest ^:benchmark benchmark-average
  (let [avg (avg/moving-average 1 1)]
    (bench "moving-average update"
      (update avg 1)))
  (let [avg (avg/moving-average 1 1)]
    (ground avg)
    (update avg 1)
    (bench "moving-average deref"
      @avg))
  (let [ch (channel)]
    (mean ch)
    (bench "mean operator"
      (enqueue ch 1))
    (close ch)))

(deftest ^:benchmark benchmark-variance
  (let [v (var/create-variance)]
    (bench "variance update"
      (update v 1)))
  (let [v (var/create-variance)]
    (update v 1)
    (bench "variance deref"
      (var/variance v)))
  (let [ch (channel)]
    (ground (variance ch))
    (bench "variance operator"
      (enqueue ch 1))
    (close ch)))

(deftest ^:benchmark benchmark-outliers
  (let [ch (channel)]
    (ground (outliers identity ch))
    (bench "outliers operator"
      (enqueue ch 1))
    (close ch)))
