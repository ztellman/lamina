;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.math
  (:use
    [lamina.core.utils])
  (:import
    [java.util Arrays]
    [java.lang.reflect Array]))

(enable-unchecked-math)

(defn ->sorted-double-array [s]
  (let [ary (double-array s)]
    (Arrays/sort ^doubles ary)
    ary))

(defn lerp ^double [^double lower ^double upper ^double t]
  (+ lower (* t (- upper lower))))

(defn lerp-array ^double [^doubles ary ^double t]
  (let [len (Array/getLength ary)]
    (cond

      (= 0 len)
      0.0

      (= 1 len)
      (aget ary 0)

      :else
      (if (== 1.0 t)
        (aget ary (dec len))
        (let [cnt (dec len)
              idx (* cnt t)
              idx-floor (int idx)
              sub-t (- idx idx-floor)]
          (lerp
            (aget ary idx-floor)
            (aget ary (inc idx-floor))
            sub-t))))))

(defn quantiles [s quantiles]
  (let [ary (->sorted-double-array s)]
    (map #(lerp-array ary %) quantiles)))
