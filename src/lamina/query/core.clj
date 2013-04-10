;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.query.core
  (:use
    [potemkin :only (definterface+)])
  (:require
    [lamina.time :as t])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

;;;

(def ^{:dynamic true} *stream-generator* nil)

(defn generate-stream [descriptor]
  (*stream-generator* descriptor))

;;;

(defonce ^ConcurrentHashMap operators (ConcurrentHashMap.))
(defonce ^ConcurrentHashMap comparators (ConcurrentHashMap.))
(defonce ^ConcurrentHashMap lookups (ConcurrentHashMap.))

(defn query-operator [name]
  (when name
    (.get operators name)))

(defn query-comparator [{:keys [name options] :as desc}]
  (when name
    (apply
      (second (.get comparators name))
      (->> options
        keys
        sort
        (map #(get options %))))))

(defn query-lookup [{:keys [name] :as desc}]
  (when name
    ((second (.get lookups name)) desc)))

;;;

(defn unwrap-key-vals [keys-val-seq]
  (->> keys-val-seq
    (mapcat
      (fn [[k v]]
        (if-not (coll? k)
          [[k v]]
          (map vector k (repeat v)))))
    (apply concat)
    (apply hash-map)))

(definterface+ ITraceOperator
  (periodic? [_])
  (distribute? [_])
  (pre-aggregate? [_])
  (transform [_ desc ch])
  (pre-aggregate [_ desc ch])
  (intra-aggregate [_ desc ch])
  (aggregate [_ desc ch]))

(defmacro def-query-operator [name & {:as args}]
  (let [{:keys [transform
                pre-aggregate
                intra-aggregate
                aggregate
                periodic?
                distribute?]} (unwrap-key-vals args)
        ns-str (str (ns-name *ns*))]
    `(let [name# ~(str name)
           transform# ~transform
           pre-aggregate# ~pre-aggregate
           intra-aggregate# ~intra-aggregate
           aggregate# ~aggregate
           periodic# ~(boolean periodic?)
           distribute# ~(boolean distribute?)
           op# (reify
                 clojure.lang.Named
                 (getName [_] name#)
                 (getNamespace [_] ~ns-str)
                 
                 lamina.query.core.ITraceOperator

                 (periodic? [_]
                   periodic#)
                 (distribute? [_]
                   distribute#)
                 (pre-aggregate? [_]
                   (boolean pre-aggregate#))
                 
                 (transform [this# desc# ch#]
                   (transform# desc# ch#))
                 (pre-aggregate [this# desc# ch#]
                   (if pre-aggregate#
                     (pre-aggregate# desc# ch#)
                     (transform# desc# ch#)))
                 (intra-aggregate [this# desc# ch#]
                   (if intra-aggregate#
                     (intra-aggregate# desc# ch#)
                     ch#))
                 (aggregate [this# desc# ch#]
                   (if aggregate#
                     (aggregate# desc# ch#)
                     (transform# desc# ch#))))]
       
       (when-let [existing-operator# (.putIfAbsent operators name# op#)]
         (if (= ~ns-str (namespace existing-operator#))
           (.put operators name# op#)
           (throw (IllegalArgumentException. (str "A query operator for '" name# "' already exists in " (namespace existing-operator#) ".")))))

       op#)))

(defmacro def-query-comparator [name & body]
  `(let [name# ~(str name)
         f# (do ~@body)]
     (when-let [[namespace# _#] (.putIfAbsent comparators name# [(str *ns*) f#])]
       (if (= namespace# (str *ns*))
         (.put comparators name# [namespace# f#])
         (throw (IllegalArgumentException. (str "A query comparator for '" name# "' already exists in " namespace# ".")))))))

(defmacro def-query-lookup [name & body]
  `(let [name# ~(str name)
         f# (do ~@body)]
     (when-let [[namespace# _#] (.putIfAbsent lookups name# [(str *ns*) f#])]
       (if (= namespace# (str *ns*))
         (.put lookups name# [namespace# f#])
         (throw (IllegalArgumentException. (str "A query lookup for '" name# "' already exists in " namespace# ".")))))))

;;;

(defn group-by? [{:keys [name] :as op}]
  (when op
    (= "group-by" name)))

(defn operator-seq [ops]
  (mapcat
    (fn [{:keys [operators] :as op}]
      (if (group-by? op)
        (cons op (operator-seq operators))
        [op]))
    ops))

(defn distributable-chain
  "Operators which can be performed at the leaves of the topology."
  [ops]
  (loop [acc [], ops ops]
    (if (empty? ops)
      acc
      (let [{:keys [operators name] :as op} (first ops)]
        (if (group-by? op)
            
          ;; traverse the group-by, see if it has to terminate mid-stream
          (let [operators* (distributable-chain operators)]
            (if (= operators operators*) 
              (recur (conj acc op) (rest ops))
              (conj acc (assoc op :operators operators*))))
            
          (if (distribute? (query-operator name))
            (recur (conj acc op) (rest ops))
            (concat
              acc
              (when (pre-aggregate? (query-operator name))
                [(assoc op :stage :pre-aggregate)]))))))))

(defn non-distributable-chain
  "Operators which must be performed at the root of the topology."
  [ops]
  (loop [ops ops]
    (when-not (empty? ops)
      (let [{:keys [operators name] :as op} (first ops)]
        (if (group-by? op)
              
          ;; traverse the group-by, see if it has to terminate mid-stream
          (let [operators* (non-distributable-chain operators)]
            (if (= operators operators*)
              (recur (rest ops))
              (list*
                (assoc op
                  :stage :aggregate
                  :operators operators*)
                (rest ops))))
              
          (if (distribute? (query-operator name))
            (recur (rest ops))
            (concat
              [(assoc op :stage :aggregate)]
              (rest ops))))))))

(defn periodic-chain?
  "Returns true if operators emit traces periodically, rather than synchronously."
  [ops]
  (->> ops
    operator-seq
    (map :name)
    (remove nil?)
    (map query-operator)
    (some periodic?)
    boolean))

(defn transform-trace-stream
  ([{:keys [name operators stage]
     :or {stage :transform}
     :as desc}
    ch]
     (let [f (case stage
               :pre-aggregate pre-aggregate
               :aggregate aggregate
               :transform transform)]

       (cond
         name
         (if-let [op (query-operator name)]
           (f op desc ch)
           (throw (IllegalArgumentException. (str "Don't recognize query operator '" name "'"))))
           
         (not (empty? operators))
         (->> operators
           (reduce #(transform-trace-stream %2 %1) ch))
           
         :else
         ch))))
