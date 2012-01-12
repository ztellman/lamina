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
  "Returns the clojure function instance name implied by the bytecode class name."
  [instance-name]
  (reduce
   (fn [base-name [pattern sub]] (str/replace base-name pattern sub))
   instance-name
   clojure-fn-subs))

;;; end clj-stacktrace

(defn function-description [f]
  (when f
    (let [f (or (operator-predicate f) f)
          s (str f)
          ns (clojure-ns s)
          f (clojure-fn s)
          anon? (clojure-anon-fn? s)]
      (when-not (and (= "identity" f) (= "clojure.core" ns))
        (str
          (when-not (= "clojure.core" ns) (str ns "/"))
          f
          (when anon? "[\u03BB]"))))))

;;;

(defn node-data [n]
  (let [f (cond
            (node? n) (.operator ^Node n)
            (callback-node? n) (.callback ^CallbackNode n)
            :else nil)]
    (merge
      {:node n
       :description (or (description n) (function-description f))}
      (when (node? n)
        {:messages (when (queue n) (-> n queue q/messages))
         :predicate? (boolean (operator-predicate f))
         :closed? (closed? n)
         :drained? (drained? n)
         :error (error-value n nil)}))))

;;;

(defn downstream-nodes [n]
  (map #(.node ^Edge %) (downstream n)))

(defn- walk-nodes- [f exclusive? n]
  (let [s (downstream-nodes n)]
    (->> s (filter node?) (l/acquire-all exclusive?))
    (when (node? n)
      (f n)
      (if exclusive?
        (l/release-exclusive n)
        (l/release n)))
    (doseq [n s]
      (walk-nodes- f exclusive? n))))

(defn walk-nodes [f n]
  (l/acquire-exclusive n)
  (walk-nodes- f true n))

(defn node-seq [n]
  (tree-seq
    (comp seq downstream-nodes)
    downstream-nodes
    n))

(defn edge-seq [n]
  (tree-seq
    #(-> % :dst :node downstream seq)
    (fn [n]
      (let [n (-> n :dst :node)
            data (node-data n)]
        (map
          (fn [^Edge e]
            {:src data
             :description (description e)
             :dst (node-data (.node e))})
          (downstream n))))
    {:dst (node-data n)}))



