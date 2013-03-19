;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.viz.core
  (:use
    [clojure.java.shell :only (sh)])
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str])
  (:import
    [javax.imageio
     ImageIO]
    [javax.swing
     JFrame JLabel JScrollPane ImageIcon]))

;;;

(defn gen-frame [name]
  (delay
    (let [frame (JFrame. ^String name)
          image-icon (ImageIcon.)]
      (doto frame
        (.add (-> image-icon JLabel. JScrollPane.))
        (.setSize 1024 480)
        (.setDefaultCloseOperation javax.swing.WindowConstants/HIDE_ON_CLOSE))
      {:frame frame
       :image-icon image-icon})))

(defn view-image [frame image]
  (let [{:keys [^JFrame frame ^ImageIcon image-icon]} @frame]
    (.setImage image-icon image)
    (.setVisible frame true)
    (java.awt.EventQueue/invokeLater
      #(doto frame
         (.setAlwaysOnTop true)
         .toFront
         .repaint
         .requestFocus
         (.setAlwaysOnTop false)))))

(defn render-dot-string [s]
  (let [bytes (:out (sh "dot" "-Tpng" :in s :out-enc :bytes))]
    (ImageIO/read (io/input-stream bytes))))


;;;

;;(def escapable-characters "|{}\"")
(def escapable-characters "\"")

(defn escape-string [s]
  (reduce
    #(str/replace %1 (str %2) (str "\\" %2))
    s
    escapable-characters))

(defn format-options-value [v]
  (cond
    (string? v) (str \" (escape-string v) \")
    (keyword? v) (name v)
    (coll? v) (str "\""
                (->> v
                  (map format-options-value)
                  (interpose ",")
                  (apply str))
                "\"")
    :else (str v)))

(defn format-options [separator m]
  (->> m
    (remove (comp nil? second))
    (map
      (fn [[k v]]
        (str (name k) "=" (format-options-value v))))
    (interpose ", ")
    (apply str)))

(defn format-edge [src dst options]
  (str src " -> " dst "["
    (->>
      [:label
       :style
       :shape
       :ltail
       :lhead
       :arrowhead
       :fontname]
      (select-keys options)
      (format-options ", "))
    "]"))

(defn format-node [id options]
  (str id "["
    (->>
      [:label
       :fontcolor
       :color
       :width
       :height
       :fontname
       :arrowhead
       :style
       :shape
       :peripheries]
      (select-keys options)
      (format-options ", "))
    "]"))

(defn digraph [{:keys
                [default-node ;; default settings for nodes
                 default-edge ;; default settings for edges
                 hierarchy ;; the subgraph hierarchy
                 subgraph->nodes ;; a map of subgraphs onto the member nodes
                 options ;; top-level options
                 nodes ;; a map of nodes onto node options
                 edges ;; a list of maps containing :src, :dst, and edge options
                 ]}]
  (let [id (memoize #(when % (gensym "node")))
        cluster-id (memoize #(when % (gensym "cluster")))]
    (letfn [(subgraph [[this & subgraphs]]
              (str "subgraph " (cluster-id this) " {\n"
                (->> this
                  subgraph->nodes
                  (filter nodes)
                  (map #(format-node (id %) (nodes %)))
                  (interpose "\n")
                  (apply str))
                "\n"
                (->> subgraphs
                  (map subgraph)
                  (apply str))
                "}\n"))]
      (str "digraph {\n"
        (when-not (empty? options)
          (str (format-options "\n" options) "\n"))
        (when default-node
          (str (format-node "node" default-node) "\n"))
        (when default-edge
          (str (format-node "edge" default-edge) "\n"))

        ;; subgraphs
        (->> hierarchy
          (map subgraph)
          (apply str))
        
        ;; nodes not contained in a subgraph
        (->> (keys nodes)
          (remove (->> subgraph->nodes vals (apply concat) set))
          (filter nodes)
          (map #(format-node (id %) (nodes %)))
          (interpose "\n")
          (apply str))
        "\n"

        ;; edges
        (->> edges
          (filter #(and (nodes (:src %)) (nodes (:dst %))))
          (map #(update-in % [:ltail] cluster-id))
          (map #(update-in % [:lhead] cluster-id))
          (map #(format-edge (id (:src %)) (id (:dst %)) %))
          (interpose "\n")
          (apply str))

        "\n}"))))

