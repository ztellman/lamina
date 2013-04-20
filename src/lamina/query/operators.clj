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

;;

(defn ordered-vals [m]
  (->> m keys sort (map #(get m %))))

;; lookups

(defn getter [x]
  (if (map? x)
    (q/query-lookup x)
    (let [str-facet (name x)]
      (cond
        (= "_" str-facet)
        identity
        
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
    (fn [m]
      (zipmap
        ks
        (map #(% m) vs)))))

(q/def-query-lookup lookup

  (fn [{:keys [options] :as desc}]
    (-> options first val getter)))

(q/def-query-lookup tuple

  (fn [{:keys [options]}]
    (let [fs (->> options ordered-vals (map getter))]
      (fn [m]
        (vec (map #(% m) fs))))))

(q/def-query-lookup get-in

  (fn [{:keys [options]}]
    (let [fs (->> options
               ordered-vals
               (map getter))]
      (fn [m]
        (loop [x m, fs fs]
          (if (empty? fs)
            x
            (if (map? x)
              (recur ((first fs) x) (rest fs))
              nil)))))))

(q/def-query-lookup nth

  (fn [{:keys [options]}]
    (let [idx (get options 0)]
      #(nth % idx))))

;; comparators

(defn normalize-for-comparison [x]
  (if (keyword? x)
    (name x)
    x))

(q/def-query-comparator <
  (fn [field value]
    (let [f (comp normalize-for-comparison (getter field))]
      #(< (f %) value))))

(q/def-query-comparator =
  (fn [field value]
    (let [f (comp normalize-for-comparison (getter field))]
      #(= (f %) value))))

(q/def-query-comparator >
  (fn [field value]
    (let [f (comp normalize-for-comparison (getter field))]
      #(> (f %) value))))

(q/def-query-comparator "~="
  (fn [field value]
    (let [f (getter field)
          value (-> value (str/replace "*" ".*") Pattern/compile)]
      #(->> %
         f
         normalize-for-comparison
         str
         (re-find value)
         boolean))))

(q/def-query-comparator in
  (fn [field value]
    (let [f (comp normalize-for-comparison (getter field))
          values (if (set? value)
                   value
                   (->> value :options vals (map normalize-for-comparison) set))]
      #(contains? values (f %)))))

;;;

(q/def-query-operator lookup
  :periodic? false
  :distribute? true

  :transform
  (fn [{:keys [options] :as desc} ch]
    (map*
      (getter
        (or
          (get options :field)
          (get options 0)))
      ch)))

(q/def-query-operator select
  :periodic? false
  :distribute? true

  :transform
  (fn [{:keys [options]} ch]
    (map* (selector options) ch)))

;;; merge, zip

(q/def-query-operator merge
  :periodic? false
  :distribute? false

  :transform
  (fn [{:keys [options]} ch]
    (let [descs (vals options)]
      (assert (every? #(contains? % :operators) descs))
      (let [chs (map
                  (fn [{:keys [operators pattern] :as desc}]
                    (if pattern
                      (q/generate-stream desc)
                      (q/transform-stream desc (fork ch))))
                  descs)]
        (apply merge-channels chs)))))

(q/def-query-operator zip
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
                        (q/transform-stream desc (fork ch)))))
                  zip
                  (map* #(zipmap ks %)))]
        (ground ch)
        ch*))))

;;; where

(defn filters [filters]
  (fn [x]
    (->> filters
      (map #(% x))
      (every? identity))))

(q/def-query-operator where
  :periodic? false
  :distribute? true
  
  :transform
  (fn [{:keys [options]} ch]
    (filter*
      (->> options
        vals
        (map q/query-comparator)
        filters)
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

    (let [ch* ch] (distribute-aggregate
       {:facet (getter facet)
        :generator (fn [k ch]
                     (let [ch (->> ch
                                (close-on-idle expiration)
                                (q/transform-stream (dissoc desc :name)))]
                       (if-not periodic?
                         (partition-every {:period period} ch)
                         ch)))
        :period period}
       ch))))

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
                            ch (q/transform-stream (dissoc desc :name) ch)]
                        (if-not periodic?
                          (partition-every {:period period} ch)
                          ch)))
         :period period}))))

(q/def-query-operator group-by
  :periodic? true
  :distribute? false
  
  :transform group-by-op
  :aggregate merge-group-by)

;;;

(defn normalize-options [{:keys [options] :as desc}]
  options)

(defn sum-op [desc ch]
  (lamina.stats/sum (normalize-options desc) ch))

(q/def-query-operator sum
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate :aggregate) sum-op)

(q/def-query-operator rolling-sum
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate :aggregate)
  (fn [desc ch]
    (->> ch
      (lamina.stats/sum (normalize-options desc))
      (reductions* +))))

(defn rate-op [desc ch]
  (lamina.stats/rate (normalize-options desc) ch))

(q/def-query-operator rate
  :periodic? true
  :distribute? false
  
  (:transform :pre-aggregate) rate-op
  :aggregate
  (fn [desc ch]
    (->> ch
      (sum-op desc)
      (map* long))))

(q/def-query-operator moving-average
  :periodic? true
  :distribute? false

  :transform
  (fn [desc ch]
    (lamina.stats/moving-average (normalize-options desc) ch)))

(q/def-query-operator moving-quantiles
  :periodic? true
  :distribute? false

  :transform
  (fn [desc ch]
    (lamina.stats/moving-quantiles (normalize-options desc) ch)))

;;;

(q/def-query-operator sample-every
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

(q/def-query-operator partition-every
  :periodic? true
  :distribute? true

  :transform partition-every-op
  :aggregate (fn [desc ch]
               (->> ch concat* (partition-every-op desc))))

;;;

(q/def-query-operator concat
  :periodic? false
  :distribute? true
  :transform (fn [_ ch] (concat* ch)))

(q/def-query-operator recur
  :periodic? true
  :distribute? true
  :transform (fn [_ ch]
               (q/transform-stream q/*query* ch)))

(q/def-query-operator nth
  :periodic? false
  :distribute? true
  :transform (fn [{:keys [options]} ch]
               (let [idx (get options 0)]
                 (map* #(nth % idx) ch))))












