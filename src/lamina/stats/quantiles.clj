;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.quantiles
  (:use
    [potemkin]
    [lamina.stats.utils])
  (:require
    [lamina.time :as t])
  (:import
    [com.yammer.metrics.core
     Clock]
    [com.yammer.metrics.stats
     ExponentiallyDecayingSample
     Snapshot]))

(defn clock [task-queue]
  (let [start (t/now task-queue)]
    (proxy [Clock] []
      (getTick [] (long (* 1e6 (- (t/now task-queue) start))))
      (getTime [] (long (t/now task-queue))))))

(deftype MovingQuantiles
  [^ExponentiallyDecayingSample sample
   task-queue
   quantiles
   scaled-quantiles]
  IUpdatable
  (update [_ value]
    (.update sample (long value) (long (/ (t/now task-queue) 1e3))))
  clojure.lang.IDeref
  (deref [_]
    (let [snapshot (.getSnapshot sample)]
      (zipmap
        quantiles
        (map #(.getValue snapshot %) scaled-quantiles)))))

(defn moving-quantiles [task-queue window quantiles]
  ;; based on assertion in Metrics that 0.015 = 5 mins
  (let [alpha (/ 4.5e-3 window)
        ^Clock c (clock task-queue)] 
    (MovingQuantiles.
      (ExponentiallyDecayingSample. (int 1024) (double alpha) c)
      task-queue
      quantiles
      (map #(double (/ quantiles 100)) quantiles))))
