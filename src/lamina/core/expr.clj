;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.expr
  (:use
    [lamina.core channel pipeline]
    [clojure walk]))

;;;

(def special-form?
  (set '(let if do let let* fn fn* quote var throw loop recur try catch finally new)))

(def unsupported-form?
  (set '(loop recur)))

(defn constant? [x]
  (and (number? x)))

(defn first= [symbol expr]
  (and (seq? expr) (= symbol (first expr))))

(defn converge [val]
  (cond
    (result-channel? val) val
    (not (or (sequential? val) (map? val) (set? val))) (success-result val)
    :else
    (let [results (atom [])]
      (prewalk
	(fn [x]
	  (when (result-channel? x)
	    (swap! results conj x))
	  x)
	val)
      (apply run-pipeline nil
	(concat
	  (map #(constantly %) @results)
	  [(fn [_]
	     (prewalk
	       #(if (result-channel? %)
		  (wait-for-result %)
		  %)
	       val))])))))

(defn transform-fn [f]
  (if (-> f meta ::original)
    f
    ^{::original f}
    (fn [& args]
      (apply run-pipeline []
	(concat
	  (map
	    (fn [x]
	      (read-merge
		(fn []
		  (if (fn? x)
		    (transform-fn x)
		    x))
		conj))
	    args)
	  [#(apply f %)])))))

(defn valid-expr? [expr]
  (and
    (seq? expr)
    (< 1 (count expr))
    (symbol? (first expr))
    (not (special-form? (first expr)))))

(defn transform-expr [expr]
  (let [args (map vector (map #(when-not (constant? %) (gensym "arg")) (rest expr)) (rest expr))]
    `(run-pipeline []
       :executor *current-executor*
       ~@(map
	   (fn [arg]
	     `(read-merge
		(fn []
		  (let [val# ~arg]
		    (if (fn? val#)
		      (transform-fn val#)
		      val#)))
		conj))
	   (->> args (remove (complement first)) (map second)))
       (fn [[~@(->> args (map first) (remove nil?))]]
	 (~(first expr) ~@(map #(if-let [x (first %)] x (second %)) args))))))

(defn transform-if [[_ predicate true-clause false-clause]]
  `(run-pipeline ~predicate
     :executor *current-executor*
     :error-handler *current-executor*
     (fn [predicate#]
       (if predicate#
	 ~true-clause
	 ~@(when false-clause [false-clause])))))

(defn transform-throw [[_ exception]]
  `(run-pipeline ~exception
     (fn [exception#]
       (throw exception#))))

(defn transform-finally [transformed-body [_ & finally-exprs]]
  `(run-pipeline nil
     (pipeline
       :executor *current-executor*
       :error-handler
       (fn [ex#]
	 (run-pipeline nil
	   :executor *current-executor*
	   :error-handler (fn [ex##] (redirect (pipeline (fn [_#] (throw ex##))) nil))
	   (fn [_#]
	     ~@finally-exprs)
	   (fn [_#]
	     (throw ex#))))
       (fn [_#]
	 ~transformed-body))
     (fn [_#]
       ~@finally-exprs)))

(defn transform-try [[_ & exprs]]
  (let [finally-clause (when (->> exprs last (first= 'finally)) (last exprs))
	exprs (if finally-clause (butlast exprs) exprs)
	catch-clauses (->> exprs reverse (take-while #(first= 'catch %)) reverse)
	exprs (drop-last (count catch-clauses) exprs)
	transformed-body
	`(run-pipeline nil
	   :error-handler
	   (fn [ex#]
	     (try
	       (let [result# (try
			       (throw ex#)
			       ~@catch-clauses)]
		 (complete result#))
	       (catch Exception e#
		 (redirect
		   (pipeline (fn [_#] (throw e#)))
		   nil))))
	   (fn [_#]
	     ~@exprs))]
    (if finally-clause
      (transform-finally transformed-body finally-clause)
      transformed-body)))

(defn async [body]
  (let [body (postwalk
	       (fn [expr]
		 (when (and (seq? expr) (unsupported-form? (first expr)))
		   (throw (Exception. (str (first expr) " not supported within (async ...)"))))
		 (cond		   
		   (valid-expr? expr) (transform-expr expr)
		   (first= 'if expr) (transform-if expr)
		   (first= 'throw expr) (transform-throw expr)
		   (first= 'try expr) (transform-try expr)
		   :else expr))
	       (prewalk macroexpand body))]
    `(do
       ~@(butlast body)
       ~(last body))))

;;;

(def *current-executor* nil)
(def default-executor (atom nil))
(def ns-executors (atom {}))

(defn set-default-executor
  "Sets the default executor used by future*."
  [executor]
  (reset! default-executor executor))

(defn set-local-executor
  "Sets the executor used by future* when called within the current namespace."
  [executor]
  (swap! ns-executors assoc *ns* executor))

(defmacro current-executor []
  (let [ns *ns*]
    `(or
       *current-executor*
       (@ns-executors ~ns)
       @default-executor
       clojure.lang.Agent/soloExecutor)))

(defn future* [body]
  `(let [result# (result-channel)
	 executor# (current-executor)]
     (.submit executor#
       (fn []
	 (binding [*current-executor* executor#]
	   (siphon-result
	     (run-pipeline nil
	       (fn [_#]
		 ~@body))
	     result#))))
     result#))

