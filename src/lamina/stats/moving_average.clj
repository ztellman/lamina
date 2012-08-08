;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.moving-average
  (:use
    [potemkin]
    [lamina.stats.utils])
  (:import
    [java.util.concurrent.atomic
     AtomicReference]))

(set! *warn-on-reflection* true)

(deftype-once Counter [^double sum ^long cnt])

(defn update-count [^Counter counter val]
  (Counter. (double (+ (.sum counter) (double val))) (inc (.cnt counter))))

(deftype MovingAverage
  [^{:volatile-mutable true} initialized?
   ^{:volatile-mutable true :tag double} rate
   ^AtomicReference counter
   ^double alpha
   ^double interval]
  IUpdatable
  (update [_ value]
    (loop []
      (let [cnt (.get counter)]
        (when-not (.compareAndSet counter cnt (update-count cnt value))
          (recur)))))
  clojure.lang.IDeref
  (deref [_]
    (let [^Counter counter (.getAndSet counter (Counter. 0 0))
          sum (.sum counter)
          cnt (.cnt counter)
          r*  (/ sum interval (max 1 cnt))]
      (if (= 0 cnt)
        0.0
        (if initialized?
          (let [r rate]
            (set! rate (double (+ r (* alpha (- r* r))))))
          (do
            (set! initialized? true)
            (set! rate (double r*))
            (* interval rate)))))))

(defn moving-average [interval window]
  (let [alpha (- 1 (Math/exp (/ (- interval) window)))]
    (MovingAverage.
      false
      (double 0.0)
      (AtomicReference. (Counter. (double 0.0) 0))
      (double alpha)
      (double (* interval 1e6)))))
