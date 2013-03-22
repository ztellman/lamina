;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.variance
  (:use
    [potemkin]
    [lamina.stats.utils])
  (:import
    [lamina.stats.utils
     IUpdatable]
    [java.util.concurrent.atomic
     AtomicLong]))

(definterface+ IVariance
  (std-dev [_])
  (variance [_])
  (mean [_]))

;; http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance

(deftype+ Variance
  [^double mean
   ^double m2
   ^long count]
  IUpdatable
  (update [_ value]
    (let [count (inc count)
          delta (- value mean)
          mean  (+ mean (/ delta count))
          m2    (if (> count 1)
                  (+ m2 (* delta (- value mean)))
                  0)]
      (Variance. mean m2 count)))
  IVariance
  (variance [_]
    (if (> count 1)
      (/ m2 (dec count))
      0))
  (std-dev [this]
    (Math/sqrt (variance this)))
  (mean [_]
    mean))

(defn create-variance []
  (Variance. 0 0 0))



