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
  (:require
    [lamina.core.node :as n]
    [lamina.core.result :as r]
    [lamina.core.lock :as l])
  (:import
    [java.awt
     Toolkit]
    [javax.swing
     JFrame JLabel JScrollPane ImageIcon]))

;;;

(def frame
  (delay
    (let [frame (JFrame. "Lamina")
          image-icon (ImageIcon.)]
      (doto frame
        (.add (-> image-icon JLabel. JScrollPane.))
        (.setSize 1024 480)
        (.setDefaultCloseOperation javax.swing.WindowConstants/HIDE_ON_CLOSE))
      {:frame frame
       :image-icon image-icon})))

(defn view-image [bytes]
  (let [image (.createImage (Toolkit/getDefaultToolkit) bytes)
        {:keys [^JFrame frame ^ImageIcon image-icon]} @frame]
    (.setImage image-icon image)
    (.setVisible frame true)
    (java.awt.EventQueue/invokeLater
      #(doto frame
         (.setAlwaysOnTop true)
         .toFront
         .repaint
         .requestFocus
         (.setAlwaysOnTop false)))))

(defn view-graphviz [s]
  (view-image (:out (sh "dot" "-Tpng" :in s :out-enc :bytes))))

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

(defn show-queue? [n]
  (or (zero? (:downstream-count n))
    (:consumed? n)))

(defn create-node [n]
  (merge
    {:id (gensym "node")
     :queue-id (when (show-queue? n) (gensym "queue"))}
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
                 (filter :queue-id)
                 (map #(select-keys % [:id :queue-id :messages])))]
    {:nodes (vals node-map)
     :edges edges
     :queues queues}))

(defn sample-graph [root message]
  (let [nodes (node-seq root)]
    (l/acquire-all true (filter n/node? nodes))
    (try
      (let [readable-nodes (->> nodes
                             (map node-data)
                             (remove #(instance? (class identity) (:operator %)))
                             (remove show-queue?))
            results (zipmap
                      (map :node readable-nodes)
                      (map #(n/read-node (:node %)) readable-nodes))
            _  (n/propagate root message true)
            data (graph-data [root])
            message-sym (gensym "msg")]
        (doseq [r (vals results)]
          (r/error r ::cancelled))
        (-> data
          (update-in [:edges]
            (fn [edges]
              (map
                (fn [e]
                  (let [n (-> e :src :node)
                        msg (if-let [result (results n)]
                              (r/success-value result ::none)
                              ::none)
                        msg (if (= ::none msg)
                              ""
                              (pr-str msg))]
                    (assoc e :description msg)))
                edges)))
          (update-in [:nodes]
            (fn [nodes]
              (conj nodes
                {:id message-sym
                 :description (pr-str message)
                 :message? true})))
          (update-in [:edges]
            (fn [edges]
              (conj edges
                {:src-id message-sym
                 :dst-id (->> data :nodes (filter #(= root (:node %))) first :id)})))))
      (finally
        (l/release-all true (filter n/node? nodes))))))

;;;

(defn edge-string [{:keys [src-id dst-id description type]}]
  (let [hide-type? (#{"join" "split" "fork"} type)
        dotted? (= "fork" type)
        description (or description (and (not hide-type?) type))]
    (format-edge src-id dst-id
      :label (or description "")
      :style (if dotted? :dotted :normal)
      :fontname :helvetica)))

(defn node-string [{:keys [id description predicate? message?]}]
  (format-node id
    :label (or description "")
    :width (cond
             message? 0
             (not description) 0.5
             :else nil)
    :fontname :helvetica
    :shape (if message? :plaintext :box)
    :peripheries (when predicate? 2)))

(defn message-string [messages cnt]
  (let [trim? (> cnt 5)
        msgs (take (if trim? 3 5) messages)]
    (str
      (when trim?
        (str (pr-str (last messages)) "| ... | "))
      (->> msgs reverse (map pr-str) (interpose " | ") (apply str)))))

(defn queue-string [{:keys [id messages queue-id]}]
  (let [messages (seq messages)
        cnt (count messages)]
    (str
      (format-node queue-id
        :shape :Mrecord
        ;;:xlabel (when (> cnt 5) cnt)
        :fontname (if messages :helvetica :times)
        :label (str "{" (if-not messages
                          "\u2205"
                          (message-string messages cnt)) "}"))
      ";\n"
      (format-edge id queue-id
        :arrowhead :dot))))

(defn graphviz-description [{:keys [nodes edges queues]}]
  (str
    "digraph {
        rankdir=LR; pad=1;"
    (->> (map node-string nodes)
      (concat
        (map edge-string edges)
        (map queue-string queues))
      (remove empty?)
      (interleave (repeat ";\n"))
      (apply str))
    ";}"))

(defn viz [& nodes]
  (->> nodes
    graph-data
    graphviz-description
    view-graphviz
    ))

(defn trace-viz [node message]
  (->> (sample-graph node message)
    graphviz-description
    view-graphviz
    ))
