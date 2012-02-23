;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.walk
  (use
    [lamina.core node utils])
  (:require
    [lamina.core.lock :as l]
    [lamina.core.queue :as q]
    [clojure.string :as str])
  (:import
    [lamina.core.node
     Edge
     Node
     CallbackNode]))

;;;

(defn node-data [n]
  (let [f (cond
            (node? n) (.operator ^Node n)
            (callback-node? n) (.callback ^CallbackNode n)
            :else nil)]
    (merge
      {:node n
       :description (description n)
       :downstream-count (count (downstream n))}
      (when (node? n)
        {:node? true
         :operator (or (operator-predicate f) f)
         :messages (when (queue n) (-> n queue q/messages))
         :predicate? (boolean (operator-predicate f))
         :consumed? (consumed? n)
         :closed? (closed? n)
         :drained? (drained? n)
         :grounded? (grounded? n)
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
    (comp seq downstream-nodes)
    downstream-nodes
    n))

(defn edge-seq
  "Returns a list of downstream edges."
  [n]
  (cyclic-tree-seq
    #(-> % :dst downstream-nodes seq)
    (fn [e]
      (let [n (:dst e)
            data (node-data n)]
        (map
          (fn [^Edge e]
            {:src n
             :description (description e)
             :dst (.node e)})
          (downstream n))))
    {:dst n}))



