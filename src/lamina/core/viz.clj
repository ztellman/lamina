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

(defn view-graphviz [s]
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

(defn format-options [m]
  (str "["
    (->> m
      (remove (comp nil? second))
      (map
        (fn [[k v]]
          (str (name k) " = "
            (cond
              (string? v) (str \" v \")
              (keyword? v) (name v)
              :else (str v)))))
      (interpose ", ")
      (apply str))
    "]"))

(defn format-edge [src dst & {:as m}]
  (str src " -> " dst (format-options m)))

(defn format-node [id & {:as m}]
  (str id " " (format-options m)))

;;;

(defn create-node [n]
  (merge
    {:id (gensym "node")}
    n))

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
                       (assoc e
                         :src-id (-> e :src :node node-map :id)
                         :dst-id (-> e :dst :node node-map :id)))))
        queues (->> node-map
                 vals
                 (filter :node?)
                 (filter (comp zero? :downstream-count))
                 (map #(select-keys % [:id :messages])))]
    {:nodes (vals node-map)
     :edges edges
     :queues queues}))

;;;

(defn edge-string [{:keys [src-id dst-id description]}]
  (let [hide-desc? (#{"join" "split" "fork"} description)
        dotted? (= "fork" description)]
    (format-edge src-id dst-id
      :label (or (when-not hide-desc? description) "")
      :style (if dotted? :dotted :normal)
      :fontfamily :helvetica)))

(defn node-string [{:keys [id description predicate?]}]
  (format-node id
    :label (or description "")
    :fontname :helvetica
    :shape :box
    :peripheries (if predicate? 2 1)))

(defn queue-string [{:keys [id messages]}]
  (let [queue-id (gensym "queue")
        messages? (seq messages)]
    (str
      (format-node queue-id
        :shape :record
        :fontname (if messages? :helvetica :times)
        :label (str "{" (if-not messages?
                          "\u2205"
                          (->> messages reverse (interpose " | ") (apply str))) "}"))
      ";\n"
      (format-edge id queue-id :arrowhead :dot))))

(defn graphviz-description [{:keys [nodes edges queues]}]
  (str
    "digraph {
        rankdir=LR;"
    (->> (map node-string nodes)
      (concat
        (map edge-string edges)
        (map queue-string queues))
      (remove empty?)
      (interleave (repeat ";\n"))
      (apply str))
    ";}"))

(defn viz [& nodes]
  (->> nodes graph-data graphviz-description view-graphviz))
