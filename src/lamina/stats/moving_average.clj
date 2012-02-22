;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.moving-average
  (:use
    [lamina.stats.utils])
  (:import
    [java.util.concurrent.atomic
     AtomicLong]))

(set! *warn-on-reflection* true)

(def mask (dec (bit-shift-left 1 40)))
(def increment (inc mask))

(defn acc-sum [n]
  (bit-and mask n))

(defn acc-count [n]
  (bit-shift-right n 40))

(deftype MovingAverage
  [^{:volatile-mutable true} initialized?
   ^{:volatile-mutable true :tag double} rate
   ^AtomicLong acc
   ^double alpha
   ^double interval]
  IUpdatable
  (update [_ value]
    (acc-count (.addAndGet acc (bit-or value increment))))
  clojure.lang.IDeref
  (deref [_]
    (let [val (.getAndSet acc 0)
          sum (acc-sum val)
          cnt (acc-count val)
          r* (/ sum interval (max 1 cnt))]
      (if initialized?
        (let [r rate]
          (set! rate (+ r (* alpha (- r* r)))))
        (do
          (set! initialized? true)
          (set! rate r*)))
      (* interval rate))))

(defn moving-average [interval window]
  (let [alpha (- 1 (Math/exp (/ (- interval) window)))]
    (MovingAverage.
      false
      0
      (AtomicLong. 0)
      alpha
      (* interval 1e6))))
