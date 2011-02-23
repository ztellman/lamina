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
    [clojure walk pprint])
  (:import
    [lamina.core.pipeline ResultChannel]))

;;;

(defmacro await-result
  [& body]
  `(let [result# (do ~@body)]
     (if (result-channel? result#)
       @result#
       result#)))

(defmacro extract-result
  [& body]
  `(let [result# (do ~@body)]
     (if-not (result-channel? result#)
       result#
       (let [result## (dequeue (.success ^ResultChannel result#) ::none)]
	 (if-not (= ::none result##)
	   result##
	   result#)))))

;;;

(declare walk-exprs)

(def *recur-point* nil)
(def *gating-values* nil)

(defn first= [expr & symbols]
  (and
    (seq? expr)
    (symbol? (first expr))
    (some #{(symbol (name (first expr)))} symbols)))

(defn print-vals [& args]
  (doseq [a args]
    (pprint a))
  (last args))

(defn walk-bindings [f bindings]
  (vec
    (interleave
      (->> bindings (partition 2) (map first))
      (->> bindings (partition 2) (map second) (map #(walk-exprs f %))))))

(defn split-special-form [x]
  (let [x* (partition 2 1 x)
	f #(not (vector? (first %)))]
    [(cons (first x) (->> x* (take-while f) (map second)))
     (->> x* (drop-while f) (map second))]))

(defn walk-special-form [f x]
  (let [[x xs] (split-special-form x)
	binding-form? (first= x 'let 'let* 'loop 'loop*)]
    (concat
      (butlast x)
      [(if binding-form?
	 (walk-bindings f (last x))
	 (last x))]
      (f (map #(walk-exprs f %) xs)))))

(defn insert-gating-sym [gating-sym fn-form]
  (let [[args body] (split-special-form fn-form)]
    `(~@args
       (let [~gating-sym (atom [])]
	 ~@body))))

(defn walk-fn-form [f x]
  (let [pipeline-sym (gensym "loop")
	gating-sym (gensym "gating")]
    (binding [*gating-values* gating-sym]
      `(let [~pipeline-sym (atom nil)]
	 (reset! ~pipeline-sym
	   (pipeline
	     (fn [x#]
	       (apply
		 ~(binding [*recur-point* pipeline-sym]
		    (doall
		      (if (or
			    (vector? (second x))
			    (and
			      (symbol? (second x))
			      (vector? (-> x rest second))))
			(insert-gating-sym gating-sym (walk-special-form f x))
			(concat
			  (take-while symbol? x)
			  (map
			    #(insert-gating-sym gating-sym (walk-special-form f %))
			    (drop-while symbol? x))))))
		 x#))))
	 (fn [~'& args#]
	   (extract-result ((deref ~pipeline-sym) args#)))))))

(defn walk-loop-form [f x]
  (let [pipeline-sym (gensym "loop")]
    `(let [~pipeline-sym (atom nil)]
       (reset! ~pipeline-sym
	 (pipeline
	   (fn [~(vec (->> x second (partition 2) (map first)))]
	     ~@(binding [*recur-point* pipeline-sym]
		 (doall
		   (map #(walk-exprs f %) (drop 2 x)))))))
       ((deref ~pipeline-sym) ~(->> x second (partition 2) (map second) vec)))))

(defn realize [x]
  (if (sequential? x) (doall x) x))

(defn walk-exprs [f x]
  (let [f* #(walk-exprs f %)]
    (realize
      (cond
	(vector? x) (f (list* 'vector (map f* x)))
	(set? x) (f (list* 'set (map f* x)))
	(map? x) (f (list* 'hash-map (map f* (apply concat x))))
	(sequential? x) (cond
			  (first= x 'fn 'fn*) (walk-fn-form f x)
			  (first= x 'let 'let*) (walk-special-form f x)
			  (first= x 'loop 'loop*) (walk-loop-form f x)
			  (first= x 'recur) `(redirect (deref ~*recur-point*) [~@(map f* (rest x))])
			  :else (f (map f* x)))
	:else (f x)))))

;;;

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

;;;

(defn partial-macroexpand [x]
  (if (first= x 'task 'loop 'await-result)
    x
    (macroexpand x)))

(def special-forms
  '(let if do let let* fn fn* quote var throw loop loop* recur try catch finally new))

(def unsupported-form?
  (set '()))

(defn constant? [x]
  (or
    (number? x)
    (string? x)
    (nil? x)))

(defn constant-elements [x]
  (if (first= x '.)
    (list* true (constant? (second x)) true (map constant? (drop 3 x)))
    (list* true (map constant? (rest x)))))

(defn transform-fn [f]
  (if (-> f meta ::original)
    f
    ^{::original f}
    (fn [& args]
      (if (every? (complement result-channel?) args)
	(apply f args)
	(extract-result
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
	      [#(apply f %)])))))))

(defn valid-expr? [expr]
  (and
    (seq? expr)
    (< 1 (count expr))
    (symbol? (first expr))
    (not (apply first= expr special-forms))))

(defn transform-expr [expr]
  (let [args (map vector
	       (->> expr
		 constant-elements
		 rest
		 (map #(when-not % (gensym "arg"))))
	       (rest expr))
	non-constant-args (->> args (remove (complement first)))
	args (map #(if-let [x (first %)] x (second %)) args)]
    (if (empty? non-constant-args)
      expr
      `(let [~@(apply concat non-constant-args)]
	 (run-pipeline []
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
	       (map first non-constant-args))
	   (fn [[~@(->> non-constant-args (map first))]]
	     (~(first expr) ~@args)))))))

(defn transform-if [[_ predicate true-clause false-clause]]
  `(run-pipeline ~predicate
     :executor *current-executor*
     (fn [predicate#]
       (if predicate#
	 ~true-clause
	 ~@(when false-clause [false-clause])))))

(defn transform-new [[_ class-name & args]]
  `(run-pipeline []
     :executor *current-executor*
     ~@(map
	 (fn [arg] `(read-merge (constantly ~arg) conj))
	 args)
     ~(let [arg-syms (take (count args) (repeatedly gensym))]
	`(fn [[~@arg-syms]]
	   (new ~class-name ~@arg-syms)))))

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
  (let [finally-clause (when (-> exprs last (first= 'finally)) (last exprs))
	exprs (if finally-clause (butlast exprs) exprs)
	catch-clauses (->> exprs reverse (take-while #(first= % 'catch)) reverse)
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

(defn transform-lazy-seq [[_ & body]]
  `(lazy-seq (await-result ~@body)))

(defn pre-process [body]
  (prewalk
    (fn [expr]
      (if-let [processed-expr
	       (cond
		 (first= expr 'lazy-seq) (transform-lazy-seq expr))]
	(pre-process (partial-macroexpand processed-expr))
	(let [expanded-expr (partial-macroexpand expr)]
	  (if-not (= expr expanded-expr)
	    (pre-process expanded-expr)
	    expr))))
    body))

(defn async [body]
  (let [body (walk-exprs
	       (fn [expr]
		 (when (and (seq? expr) (unsupported-form? (first expr)))
		   (throw (Exception. (str (first expr) " not supported within (async ...)"))))
		 (cond		   
		   (valid-expr? expr) (transform-expr expr)
		   (first= expr 'if) (transform-if expr)
		   (first= expr 'throw) (transform-throw expr)
		   (first= expr 'try) (transform-try expr)
		   (first= expr 'new) (transform-new expr)
		   :else expr))
	       (->> body
		 pre-process
		 (prewalk partial-macroexpand)))]
    `(run-pipeline nil
       (fn [_#]
	 ~@body))))

;;;

(def *current-executor* nil)
(def default-executor (atom nil))
(def ns-executors (atom {}))

(defn set-default-executor
  "Sets the default executor used by task."
  [executor]
  (reset! default-executor executor))

(defn set-local-executor
  "Sets the executor used by task when called within the current namespace."
  [executor]
  (swap! ns-executors assoc *ns* executor))

(defmacro current-executor []
  (let [ns *ns*]
    `(or
       *current-executor*
       (@ns-executors ~ns)
       @default-executor
       clojure.lang.Agent/soloExecutor)))

(defn task [body]
  `(let [result# (result-channel)
	 executor# (current-executor)]
     (.submit executor#
       (fn []
	 (binding [*current-executor* executor#]
	   (siphon-result
	     (run-pipeline nil
	       :error-handler (constantly nil)
	       (fn [_#]
		 ~@body))
	     result#))))
     result#))

