;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.router.core
  (:use
    [lamina.cache :only (subscribe)]
    [potemkin :only (defprotocol+)])
  (:require
    [lamina.time :as t]
    [lamina.trace.context])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

;;;

(def ^{:dynamic true} *stream-generator* nil)

(def ^{:dynamic true} *period* 1000)

(defn generate-stream [descriptor]
  (*stream-generator* descriptor))

;;;

(defonce ^ConcurrentHashMap operators (ConcurrentHashMap.))

(defn operator [name]
  (when name
    (.get operators name)))

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

(defprotocol+ TraceOperator
  (periodic? [_])
  (distribute? [_])
  (pre-aggregate? [_])
  (transform [_ desc ch])
  (pre-aggregate [_ desc ch])
  (intra-aggregate [_ desc ch])
  (aggregate [_ desc ch]))

(defn populate-desc [operator desc]
  desc)

(defmacro def-trace-operator [name & {:as args}]
  (let [{:keys [transform
                pre-aggregate
                intra-aggregate
                aggregate
                periodic?
                distribute?]} (unwrap-key-vals args)
        ns-str (str (ns-name *ns*))]
    `(let [transform# ~transform
           pre-aggregate# ~pre-aggregate
           intra-aggregate# ~intra-aggregate
           aggregate# ~aggregate
           periodic# ~(boolean periodic?)
           distribute# ~(boolean distribute?)
           op# (reify
                 clojure.lang.Named
                 (getName [_] ~(str name))
                 (getNamespace [_] ~ns-str)
                 
                 TraceOperator

                 (periodic? [_]
                   periodic#)
                 (distribute? [_]
                   distribute#)
                 (pre-aggregate? [_]
                   (boolean pre-aggregate#))
                 
                 (transform [this# desc# ch#]
                   (let [desc# (lamina.trace.router.core/populate-desc this# desc#)]
                     (transform# desc# ch#)))
                 (pre-aggregate [this# desc# ch#]
                   (let [desc# (lamina.trace.router.core/populate-desc this# desc#)]
                     (if pre-aggregate#
                       (pre-aggregate# desc# ch#)
                       (transform# desc# ch#))))
                 (intra-aggregate [this# desc# ch#]
                   (let [desc# (lamina.trace.router.core/populate-desc this# desc#)]
                     (if intra-aggregate#
                       (intra-aggregate# desc# ch#)
                       ch#)))
                 (aggregate [this# desc# ch#]
                   (let [desc# (lamina.trace.router.core/populate-desc this# desc#)]
                     (if aggregate#
                       (aggregate# desc# ch#)
                       (transform# desc# ch#)))))]
       
       (when-let [existing-operator# (.putIfAbsent operators ~(str name) op#)]
         (if (= ~ns-str (namespace existing-operator#))
           (.put operators ~(str name) op#)
           (throw (IllegalArgumentException. (str "An operator for '" ~(str name) "' already exists in " (namespace existing-operator#) ".")))))

       op#)))

;;;

(defn group-by? [{:strs [name] :as op}]
  (when op
    (= "group-by" name)))

(defn operator-seq [ops]
  (mapcat
    (fn [{:strs [operators] :as op}]
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
      (let [{:strs [operators name] :as op} (first ops)]
        (if (group-by? op)
            
          ;; traverse the group-by, see if it has to terminate mid-stream
          (let [operators* (distributable-chain operators)]
            (if (= operators operators*) 
              (recur (conj acc op) (rest ops))
              (conj acc (assoc op "operators" operators*))))
            
          (if (or
                ;; we don't know what this is, maybe the endpoint does
                (not (operator name))
                (distribute? (operator name)))
            (recur (conj acc op) (rest ops))
            (concat
              acc
              (when (pre-aggregate? (operator name))
                [(assoc op "stage" "pre-aggregate")]))))))))

(defn non-distributable-chain
  "Operators which must be performed at the root of the topology."
  [ops]
  (loop [ops ops]
    (when-not (empty? ops)
      (let [{:strs [operators name] :as op} (first ops)]
        (if (group-by? op)
              
          ;; traverse the group-by, see if it has to terminate mid-stream
          (let [operators* (non-distributable-chain operators)]
            (if (= operators operators*)
              (recur (rest ops))
              (list*
                (assoc op
                  "stage" "aggregate"
                  "operators" operators*)
                (rest ops))))
              
          (if (or
                (not (operator name))
                (distribute? (operator name)))
            (recur (rest ops))
            (concat
              (when (operator name)
                [(assoc op "stage" "aggregate")])
              (rest ops))))))))

(defn periodic-chain?
  "Returns true if operators emit traces periodically, rather than synchronously."
  [ops]
  (->> ops
    operator-seq
    (map #(get % "name"))
    (remove nil?)
    (map operator)
    (some periodic?)
    boolean))

(defn transform-trace-stream
  ([{:strs [name operators stage __implicit]
     :or {stage "transform"}
     :as desc}
    ch]
     (let [f (case (keyword stage)
               :pre-aggregate pre-aggregate
               :aggregate aggregate
               :transform transform)]

       (cond
         name
         (f (operator name) desc ch)
           
         (not (empty? operators))
         (->> operators
           (map #(assoc % "__implicit" __implicit))
           (reduce #(transform-trace-stream %2 %1) ch))
           
         :else
         ch))))
