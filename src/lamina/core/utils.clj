;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.utils
  (:use
    [potemkin])
  (:require
    [clojure.tools.logging :as log]
    [clojure.string :as str]))

(defprotocol+ IEnqueue
  (enqueue [_ msg]))

(defprotocol+ IError
  (error [_ err force?]))

(defprotocol+ IDescribed
  (description [_]))

(defn in-transaction? []
  (clojure.lang.LockingTransaction/isRunning))

(defn result-seq [s]
  (with-meta (seq s) {:lamina/result-seq true}))

;;;

(defn predicate-operator [predicate]
  (with-meta
    (fn [x]
      (if (predicate x)
        x
        :lamina/false))
    {::predicate predicate}))

(defn operator-predicate [f]
  (->> f meta ::predicate))

;;;

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
