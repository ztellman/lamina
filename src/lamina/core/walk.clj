;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.walk
  (use
    [lamina.core.node])
  (:require
    [lamina.core.lock :as l]
    [lamina.core.queue :as q]
    [clojure.string :as str])
  (:import
    [lamina.core.node
     Edge
     Node
     CallbackNode]))

;; These functions are adapted from Mark McGranaghan's clj-stacktrace, which
;; is released under the MIT license and therefore amenable to this sort of
;; copy/pastery.

(defn clojure-ns
  "Returns the clojure namespace name implied by the bytecode class name."
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

(defn function-instance? [x]
  (boolean (re-matches #"^[^$]*\$[^@]*@[0-9a-f]*$" (str x))))

(defn operator-description [x]
  (cond
    (map? x)
    (str "{ ... }")

    (set? x)
    (str "#{ ... }")
    
    (not (function-instance? x))
    (pr-str x)

    :else
    (let [f (or (operator-predicate x) x)
          s (str f)
          ns (clojure-ns s)
          f (clojure-fn s)
          anon? (clojure-anon-fn? s)]
      (when-not (and (= "identity" f) (= "clojure.core" ns))
        (str
          (when-not (= "clojure.core" ns) (str ns "/"))
          f
          (when anon? "[fn]"))))))

;;;

(defn node-data [n]
  (let [f (cond
            (node? n) (.operator ^Node n)
            (callback-node? n) (.callback ^CallbackNode n)
            :else nil)]
    (merge
      {:node n
       :description (or (description n) (operator-description f))
       :downstream-count (count (downstream n))}
      (when (node? n)
        {:node? true
         :operator f
         :messages (when (queue n) (-> n queue q/messages))
         :predicate? (boolean (operator-predicate f))
         :consumed? (consumed? n)
         :closed? (closed? n)
         :drained? (drained? n)
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
    #(-> % :dst :node downstream seq)
    (fn [n]
      (let [n (-> n :dst :node)
            data (node-data n)]
        (map
          (fn [^Edge e]
            {:src data
             :type (description e)
             :dst (node-data (.node e))})
          (downstream n))))
    {:dst (node-data n)}))



