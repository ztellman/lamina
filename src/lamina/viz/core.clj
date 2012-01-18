;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.viz.core
  (:use
    [clojure.java.shell :only (sh)]
    [lamina.core.node :only (operator-predicate)])
  (:require
    [clojure.string :as str])
  (:import
    [java.awt
     Toolkit]
    [javax.swing
     JFrame JLabel JScrollPane ImageIcon]))

;;;

(defn gen-frame [name]
  (delay
    (let [frame (JFrame. name)
          image-icon (ImageIcon.)]
      (doto frame
        (.add (-> image-icon JLabel. JScrollPane.))
        (.setSize 1024 480)
        (.setDefaultCloseOperation javax.swing.WindowConstants/HIDE_ON_CLOSE))
      {:frame frame
       :image-icon image-icon})))

(defn view-image [frame bytes]
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

(defn view-dot-string [frame s]
  (view-image frame (:out (sh "dot" "-Tpng" :in s :out-enc :bytes))))


;;;

(defn format-options [separator m]
  (->> m
    (remove (comp nil? second))
    (map
      (fn [[k v]]
        (str (name k) "="
          (cond
            (string? v) (str \" (str/replace v "\\" "\\\\") \")
            (keyword? v) (name v)
            :else (str v)))))
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

;; These functions are adapted from Mark McGranaghan's clj-stacktrace, which
;; is released under the MIT license and therefore amenable to this sort of
;; copy/pastery.

(defn clojure-ns
  "Returns the clojure namespace name implied by the bytecode instance name."
  [instance-name]
  (str/replace
    (or (get (re-find #"([^$]+)\$" instance-name) 1)
      (get (re-find #"(.+)\.[^.]+$" instance-name) 1))
    #"_" "-"))

(def clojure-fn-subs
  [[#"^[^$]*\$"     ""]
   [#"\$.*"         ""]
   [#"@[0-9a-f]*$"  ""]
   [#"__\d+.*"      ""]
   [#"_QMARK_"     "?"]
   [#"_BANG_"      "!"]
   [#"_PLUS_"      "+"]
   [#"_GT_"        ">"]
   [#"_LT_"        "<"]
   [#"_EQ_"        "="]
   [#"_STAR_"      "*"]
   [#"_SLASH_"     "/"]
   [#"_"           "-"]])

(defn clojure-anon-fn?
  "Returns true if the bytecode instance name implies an anonymous inner fn."
  [instance-name]
  (boolean (re-find #"\$.*\$" instance-name)))

(defn clojure-fn
  "Returns the clojure function name implied by the bytecode instance name."
  [instance-name]
  (reduce
   (fn [base-name [pattern sub]] (str/replace base-name pattern sub))
   instance-name
   clojure-fn-subs))

;;; end clj-stacktrace

(defn fn-instance? [x]
  (boolean (re-matches #"^[^$]*\$[^@]*@[0-9a-f]*$" (str x))))

(defn describe-fn [x]
  (cond
    (map? x)
    (str "{ ... }")

    (set? x)
    (str "#{ ... }")

    (vector? x)
    (str "[ ... ]")
    
    (not (fn-instance? x))
    (pr-str x)

    :else
    (let [f (or (operator-predicate x) x)
          s (str f)
          ns (clojure-ns s)
          f (clojure-fn s)
          anon? (clojure-anon-fn? s)]
      (str
        (when-not (= "clojure.core" ns) (str ns "/"))
        f
        (when anon? "[fn]")))))
