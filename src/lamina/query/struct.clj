;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.query.struct
  (:require [lamina.time :as t]))

;; canonical structure is ["pattern" transform1 transform2 ...]
;; or just [transform1 transform2]
;; group-by is a special case, with ["pattern" [group-by :foo [transform1 transform 2 ...]]]

(defn first= [s x]
  (= (first s) x))

(def time-interval-regex #"^[0-9\.]+[d|h|ms|m|s|us|ns]")

(def interval->fn
  {"d" t/days
   "h" t/hours
   "m" t/minutes
   "s" t/seconds
   "ms" t/milliseconds
   "us" t/microseconds
   "ns" t/nanoseconds})

(defn parse-time-interval [s]
  (let [num (re-find #"^[0-9\.]+" s)
        interval (re-find #"d|h|ms|m|s|us|ns" s)]
    ((interval->fn interval)
     (read-string num))))

(declare transform-operator)
(declare parse-struct-query)

(defn flatten-options [s]
  (apply merge
    (map
      (fn [idx x]
        (let [m (if (map? x)
                  x
                  {idx x})]
          (zipmap
            (keys m)
            (map
              #(cond
                 (sequential? %)
                 (parse-struct-query %)

                 (and (keyword? %) (re-find time-interval-regex (name %)))
                 (parse-time-interval (name %))

                 :else
                 %)
              (vals m)))))
      (iterate inc 0)
      s)))

(defn transform-operator [x]
  (cond
    (or (= '_ x) (keyword? x))
    {:name "lookup"
     :options {0 x}}
    
    (symbol? x)
    {:name (str x)}

    (not (sequential? x))
    (throw (IllegalArgumentException. (str "Invalid operator '" (pr-str x) "'")))

    (first= x 'group-by)
    {:name "group-by"
     :options (->> x
                rest
                butlast
                flatten-options)
     :operators (let [ops (last x)]
                  (if (vector? ops)
                    (map transform-operator ops)
                    (transform-operator ops)))}

    :else
    {:name (str (first x))
     :options (flatten-options (rest x))}))

(defn parse-struct-query [q]
  (if (vector? q)

    ;; composed operators
    (cond
      (string? (first q))
      {:pattern (first q)
       :operators (map transform-operator (rest q))}

      (number? (first q))
      q

      :else
      {:operators (map transform-operator q)})

    ;; single operator
    (transform-operator q)))
