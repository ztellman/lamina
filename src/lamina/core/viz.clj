;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.viz
  (:use
    [lamina.core walk]
    [clojure.java.shell :only (sh)])
  (:import
    [javax.swing
     JFrame JLabel JScrollPane ImageIcon]))

;;;

;; adapted from Chris Houser's clojure-classes: https://github.com/Chouser/clojure-classes
(defn view-dot-string [s]
  (doto (JFrame. "Lamina")
    (.add (-> (sh "dot" "-Tpng" :in s :out-enc :bytes)
            :out
            ImageIcon.
            JLabel.
            JScrollPane.))
   (.setSize 640 480)
   (.setDefaultCloseOperation javax.swing.WindowConstants/DISPOSE_ON_CLOSE)
   (.setVisible true)))

;;;

(defn create-node [n]
  {:id (gensym "node")
   :description (:description n)
   :messages (:messages n)})

(defn update-node-map [m n]
  (let [k (:node n)]
    (if (contains? m k)
     m
     (assoc m k (create-node n)))))

(defn graph-data [nodes]
  (let [edges (mapcat edge-seq nodes)
        node-map (->> edges
                   (mapcat #(vector (:src %) (:dst %)))
                   (remove nil?)
                   (reduce update-node-map {}))
        edges (->> edges
                (filter :src)
                (map (fn [e]
                       (-> e
                         (update-in [:src] #(-> % :node node-map :id))
                         (update-in [:dst] #(-> % :node node-map :id))))))]
    {:nodes (vals node-map)
     :edges edges}))

(defn edge-string [{:keys [src dst description]}]
  (str src " -> " dst "[label=\"" description "\"]"))

(defn node-string [{:keys [id description]}]
  (str id " [label=\"" description "\", shape=box]"))

(defn gen-dot-string [nodes]
  (let [{:keys [nodes edges]} (graph-data nodes)]
    (str
      "digraph {
        rankdir=LR;\n"
      (apply str
        (interleave
          (concat
            (map edge-string edges)
            (map node-string nodes))
          (repeat ";\n")))
      "}")))

(defn viz [& nodes]
  (->> nodes gen-dot-string view-dot-string))
