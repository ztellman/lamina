;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.router.operators
  (:use
    [lamina core])
  (:require
    [lamina.trace.router.core :as r]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [lamina.trace.context]
    [lamina.stats])
  (:import
    [java.util.regex
     Pattern]))

;; lookups

(defn keywordize [m]
  (zipmap
    (map keyword (keys m))
    (vals m)))

(defn getter [lookup]
  (if (coll? lookup)

    ;; do tuple lookup
    (let [fs (map getter lookup)]
      (fn [m]
        (vec (map #(% m) fs))))

    (let [str-facet (-> lookup name)]
      (cond
        (= "_" str-facet)
        identity
        
        #_(= "_origin" str-facet)
        #_(fn [m]
          (origin))
        
        ;; do path lookup
        (re-find #"\." str-facet)
        (let [fields (map getter (str/split str-facet #"\."))]
          (fn [m]
            (reduce
              (fn [m f]
                (when (map? m)
                  (f m)))
              m
              fields)))
        
        ;; do normal lookup
        :else
        (let [key-facet (keyword str-facet)]
          (fn [m]
            (if (contains? m str-facet)
              (get m str-facet)
              (get m key-facet))))))))

(defn selector [m]
  (let [ignore-key? #(re-find #"^[0-9]+" %)
        ks (map (fn [[k v]] (if (ignore-key? k) v k)) m)
        vs (->> m vals (map getter))]
    (assert (every? #(not (re-find #"\." %)) ks))
    (fn [m]
      (zipmap
        ks
        (map #(% m) vs)))))

(r/def-trace-operator lookup
  :periodic? false
  :distribute? true

  :transform
  (fn [{:strs [options] :as desc} ch]
    (map* (getter (get options "field")) ch)))

(r/def-trace-operator select
  :periodic? false
  :distribute? true

  :transform
  (fn [{:strs [options]} ch]
    (map* (selector options) ch)))

;;; merge, zip

(r/def-trace-operator merge
  :periodic? false
  :distribute? false

  :transform
  (fn [{:strs [options]} ch]
    (let [options (dissoc options "period")
          descs (vals options)]
      (assert (every? #(contains? % "operators") descs))
      (apply merge-channels
        (map
          (fn [{:strs [operators pattern] :as desc}]
            (if pattern
              (r/generate-stream desc)
              (r/transform-trace-stream desc (fork ch))))
          descs)))))

(r/def-trace-operator zip
  :periodic? false
  :distribute? false

  :transform
  (fn [{:strs [options]} ch]
    (let [options (dissoc options "period")
          ks (keys options)
          descs (vals options)]
      (assert (every? #(contains? % "operators") descs))
      (->> (apply zip
             (map
               (fn [{:strs [operators pattern] :as desc}]
                 (if pattern
                   (r/generate-stream desc)
                   (r/transform-trace-stream desc (fork ch))))
               descs))
        (map* #(zipmap ks %))))))

;;; where

(defn normalize-for-comparison [x]
  (if (keyword? x)
    (name x)
    x))

(defn comparison-filter [[a comparison b]]
  (assert (and a comparison b))
  (let [a (getter a)]
    (case comparison
      "=" #(= (normalize-for-comparison (a %)) b)
      "<" #(< (a %) b)
      ">" #(> (a %) b)
      "~=" (let [b (-> b (str/replace "*" ".*") Pattern/compile)]
             #(->> (a %)
                normalize-for-comparison
                str
                (re-find b)
                boolean)))))

(defn filters [filters]
  (fn [x] (->> filters (map #(% x)) (every? identity))))

(r/def-trace-operator where
  :periodic? false
  :distribute? true
  
  :transform
  (fn [{:strs [options]} ch]
    (filter* (filters (->> options vals (map comparison-filter))) ch)))

;;; group-by

(defn group-by-op [{:strs [options operators] :as desc} ch]
  (let [expiration (get options "expiration" (* 1000 60))
        facet (or
                (get options "facet")
                (get options "0"))
        periodic? (r/periodic-chain? operators)]
    (assert facet)
    (distribute-aggregate
      (getter facet)
      (fn [k ch]
        (let [ch (->> ch
                   (close-on-idle expiration)
                   (r/transform-trace-stream {"operators" operators}))]
          (if-not periodic?
            (partition-every 1000 ch)
            ch)))
      ch)))

(defn merge-group-by [{:strs [options operators] :as desc} ch]
  (let [expiration (get options "expiration" (* 1000 60))
        periodic? (r/periodic-chain? operators)]
    (->> ch
      concat*
      (distribute-aggregate first
        (fn [k ch]
          (let [ch (->> ch
                     (close-on-idle expiration)
                     (map* second))
                ch (if-not periodic?
                     (concat* ch)
                     ch)
                ch (r/transform-trace-stream {"operators" operators} ch)]
            (if-not periodic?
              (partition-every 1000 ch)
              ch)))))))

(r/def-trace-operator group-by
  :periodic? true
  :distribute? false
  
  :transform group-by-op
  :aggregate merge-group-by)

;;;

(defn sum-op [{:strs [options]} ch]
  (lamina.stats/sum (keywordize options) ch))

(r/def-trace-operator sum
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate :aggregate) sum-op)

(defn rate-op [{:strs [options]} ch]
  (lamina.stats/rate (keywordize options) ch))

(r/def-trace-operator rate
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate) rate-op
  :aggregate sum-op)

(r/def-trace-operator moving-average
  :periodic? true
  :distribute? false

  :transform
  (fn [{:strs [options]} ch]
    (lamina.stats/moving-average (keywordize options) ch)))

;;;

(r/def-trace-operator sample
  :periodic? true
  :distribute? true

  :transform
  (fn [{:strs [options]} ch]
    (let [period (or
                   (get options "0")
                   (get options "period"))]
      (sample-every period ch))))

(defn partition-every-op
  [{:strs [options]} ch]
  (let [period (or
                 (get options "0")
                 (get options "period"))]
    (partition-every period ch)))

(r/def-trace-operator partition-every
  :periodic? true
  :distribute? true

  :transform partition-every-op
  :aggregate (fn [desc ch]
               (->> ch concat* (partition-every-op desc))))













