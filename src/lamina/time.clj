;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.time
  (:require
    [clojure.string :as str])
  (:import
    [java.util
     Calendar
     TimeZone]))

(defn now []
  (System/currentTimeMillis))

(defn nanoseconds
  "Converts nanoseconds -> milliseconds"
  [n]
  (/ n 1e6))

(defn microseconds
  "Converts microseconds -> milliseconds"
  [n]
  (/ n 1e3))

(defn milliseconds
  "Converts milliseconds -> milliseconds"
  [n]
  n)

(defn seconds
  "Converts seconds -> milliseconds"
  [n]
  (* n 1e3))

(defn minutes
  "Converts minutes -> milliseconds"
  [n]
  (* n 6e4))

(defn hours
  "Converts hours -> milliseconds"
  [n]
  (* n 36e5))

(defn days
  "Converts days -> milliseconds"
  [n]
  (* n 864e5))

(defn hz
  "Converts frequency -> period in milliseconds"
  [n]
  (/ 1e3 n))

(let [intervals (partition 2
                  ["d" (days 1)
                   "h" (hours 1)
                   "m" (minutes 1)
                   "s" (seconds 1)])]

  (defn format-duration
    "Returns a formatted string describing an interval, i.e. '5d 3h 1m'"
    [n]
    (loop [s "", n n, intervals intervals]
      (if (empty? intervals)
        (if (empty? s)
          "0s"
          (str/trim s))
        (let [[desc val] (first intervals)]
          (if (>= n val)
            (recur
              (str s (int (/ n val)) desc " ")
              (rem n val)
              (rest intervals))
            (recur s n (rest intervals))))))))

(let [sorted-intervals [:millisecond Calendar/MILLISECOND
                        :second Calendar/SECOND
                        :minute Calendar/MINUTE
                        :hour Calendar/HOUR
                        :day Calendar/DAY_OF_YEAR
                        :week Calendar/WEEK_OF_MONTH
                        :month Calendar/MONTH]
      interval->calendar-intervals (apply hash-map sorted-intervals)
      intervals (->> sorted-intervals (partition 2) (map first))
      interval->cleared-fields (zipmap
                                 intervals
                                 (map
                                   #(->> (take % intervals) (map interval->calendar-intervals))
                                   (range (count intervals))))]

  (defn floor [timestamp interval]
    (assert (contains? interval->calendar-intervals interval))
    (let [^Calendar cal (doto (Calendar/getInstance (TimeZone/getTimeZone "UTC"))
                          (.setTimeInMillis timestamp))]
      (doseq [field (interval->cleared-fields interval)]
        (.set cal field 0))
      (.getTimeInMillis cal))))


