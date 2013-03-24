;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.query.operators
  (:use
    [lamina core])
  (:require
    [lamina.query.core :as q]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [lamina.time :as t]
    [lamina.stats])
  (:import
    [java.util.regex
     Pattern]))

;; lookups

(defn getter [lookup]
  (if (coll? lookup)

    ;; do tuple lookup
    (let [fs (map getter lookup)]
      (fn [m]
        (vec (map #(% m) fs))))

    ;; do field lookup
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
  (let [ignore-key? number?
        ks (map (fn [[k v]] (if (ignore-key? k) v k)) m)
        vs (->> m vals (map getter))]
    (assert (every? #(not (re-find #"\." (name %))) ks))
    (fn [m]
      (zipmap
        ks
        (map #(% m) vs)))))

(q/def-trace-operator lookup
  :periodic? false
  :distribute? true

  :transform
  (fn [{:keys [options] :as desc} ch]
    (map* (getter (get options :field)) ch)))

(q/def-trace-operator select
  :periodic? false
  :distribute? true

  :transform
  (fn [{:keys [options]} ch]
    (map* (selector options) ch)))

;;; merge, zip

(q/def-trace-operator merge
  :periodic? false
  :distribute? false

  :transform
  (fn [{:keys [options]} ch]
    (let [descs (vals options)]
      (assert (every? #(contains? % :operators) descs))
      (apply merge-channels
        (map
          (fn [{:keys [operators pattern] :as desc}]
            (if pattern
              (q/generate-stream desc)
              (q/transform-trace-stream desc (fork ch))))
          descs)))))

(q/def-trace-operator zip
  :periodic? true
  :distribute? false

  :transform
  (fn [{:keys [options]} ch]
    (let [options options
          ks (map keyword (keys options))
          descs (vals options)]
      (assert (every? #(contains? % :operators) descs))
      (let [ch* (->> descs
                  (map
                    (fn [{:keys [operators pattern] :as desc}]
                      (if pattern
                        (q/generate-stream desc)
                        (q/transform-trace-stream desc (fork ch)))))
                  zip
                  (map* #(zipmap ks %)))]
        (ground ch)
        ch*))))

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

(q/def-trace-operator where
  :periodic? false
  :distribute? true
  
  :transform
  (fn [{:keys [options]} ch]
    (filter*
      (->> options vals (map comparison-filter) filters)
      ch)))

;;; group-by

(defn group-by-op [{:keys [options operators] :as desc} ch]
  (let [facet (or (get options :facet)
                (get options 0))
        periodic? (q/periodic-chain? operators)
        period (or (get options :period)
                 (t/period))
        expiration (get options :expiration (max (t/minutes 1) (* 10 period)))]

    (assert facet)

    (distribute-aggregate
      {:facet (getter facet)
       :generator (fn [k ch]
                    (let [ch (->> ch
                               (close-on-idle expiration)
                               (q/transform-trace-stream (dissoc desc :name)))]
                      (if-not periodic?
                        (partition-every {:period period} ch)
                        ch)))
       :period period}
      ch)))

(defn merge-group-by [{:keys [options operators] :as desc} ch]
  (let [periodic? (q/periodic-chain? operators)
        period (or (get options :period)
                 (t/period))
        expiration (get options :expiration (max (t/minutes 1) (* 10 period)))]
    (->> ch
      concat*
      (distribute-aggregate
        {:facet first
         :generator (fn [k ch]
                      (let [ch (->> ch
                                 (close-on-idle expiration)
                                 (map* second))
                            ch (if-not periodic?
                                 (concat* ch)
                                 ch)
                            ch (q/transform-trace-stream (dissoc desc :name) ch)]
                        (if-not periodic?
                          (partition-every {:period period} ch)
                          ch)))
         :period period}))))

(q/def-trace-operator group-by
  :periodic? true
  :distribute? false
  
  :transform group-by-op
  :aggregate merge-group-by)

;;;

(defn normalize-options [{:keys [options] :as desc}]
  options)

(defn sum-op [desc ch]
  (lamina.stats/sum (normalize-options desc) ch))

(q/def-trace-operator sum
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate :aggregate) sum-op)

(q/def-trace-operator rolling-sum
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate :aggregate)
  (fn [desc ch]
    (->> ch
      (lamina.stats/sum (normalize-options desc))
      (reductions* +))))

(defn rate-op [desc ch]
  (lamina.stats/rate (normalize-options desc) ch))

(q/def-trace-operator rate
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate) rate-op
  :aggregate
  (fn [desc ch]
    (->> ch
      (sum-op desc)
      (map* long))))

(q/def-trace-operator moving-average
  :periodic? true
  :distribute? false

  :transform
  (fn [desc ch]
    (lamina.stats/moving-average (normalize-options desc) ch)))

(q/def-trace-operator moving-quantiles
  :periodic? true
  :distribute? false

  :transform
  (fn [desc ch]
    (lamina.stats/moving-quantiles (normalize-options desc) ch)))

;;;

(q/def-trace-operator sample-every
  :periodic? true
  :distribute? true

  :transform
  (fn [{:keys [options] :as desc} ch]
    (let [period (or (get options :period)
                   (get options 0)
                   (t/period))]
      (sample-every {:period period} ch))))

(defn partition-every-op
  [{:keys [options] :as desc} ch]
  (let [period (or (get options :period)
                 (get options 0)
                 (t/period))]
    (partition-every {:period period} ch)))

(q/def-trace-operator partition-every
  :periodic? true
  :distribute? true

  :transform partition-every-op
  :aggregate (fn [desc ch]
               (->> ch concat* (partition-every-op desc))))













