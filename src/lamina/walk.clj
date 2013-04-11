;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.walk
  (use
    [lamina.core graph utils])
  (:require
    [lamina.core.lock :as l]
    [lamina.core.queue :as q]
    [clojure.string :as str])
  (:import
    [lamina.core.graph.node
     Node NodeState]
    [lamina.core.graph.core
     Edge]
    [lamina.core.graph.propagator
     CallbackPropagator]))

;;;

(defn node-data [n]
  (let [f (cond
            (node? n) (.operator ^Node n)
            (instance? CallbackPropagator n) (.callback ^CallbackPropagator n)
            :else nil)]
    (merge
      {:node n
       :description (description n)
       :downstream-count (count (downstream n))}
      (when (node? n)
        {:node? true
         :downstream-count (.downstream-count ^NodeState (.state ^Node n))
         :queue-size (count n)
         :operator (or (operator-predicate f) f)
         :messages (when (queue n false) (-> n (queue false) q/messages))
         :predicate? (boolean (operator-predicate f))
         :consumed? (consumed? n)
         :closed? (closed? n)
         :drained? (drained? n)
         :grounded? (grounded? n)
         :permanent (permanent? n)
         :error (error-value n nil)}))))

;;;

(defn cyclic-tree-seq
  "Version of tree-seq that doesn't infinitely recur when
   there are cycles."
  [branch? children root]
  (let [seen? (atom #{})]
    (tree-seq
      #(when-not (@seen? %)
         (swap! seen? conj %)
         (branch? %))
      #(remove @seen? (children %))
      root)))

(defn node-seq
  "Returns a list of downstream nodes."
  [n]
  (cyclic-tree-seq
    (comp seq downstream-propagators)
    downstream-propagators
    n))

(defn edge-seq
  "Returns a list of downstream edges."
  [n]
  (cyclic-tree-seq
    #(-> % :dst downstream-propagators seq)
    (fn [e]
      (let [n (:dst e)
            data (node-data n)]
        (map
          (fn [^Edge e]
            {:src n
             :description (description e)
             :dst (.next e)})
          (downstream n))))
    {:dst n}))



