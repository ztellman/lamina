;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.expr
  (:use
    clojure.walk
    [lamina.core.expr walk utils task tag]
    [lamina.core channel pipeline utils]))

;;;

(def *debug* false)

(defmacro debug-print [& args]
  (when *debug*
    `(println ~@args)))

(defn partial-macroexpand [x]
  (if (first= x 'task 'loop 'await-result)
    x
    (let [ex (macroexpand-1 x)]
      (if-not (identical? ex x)
	(partial-macroexpand ex)
	x))))

(defn transform-fn [f]
  (if (-> f meta original-fn)
    f
    ^{original-fn f}
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
	non-constant-args (->> args (remove (complement first)))]
    (if (empty? non-constant-args)
      `(let [result# ~expr]
	 (debug-print ~@(map str expr) "=" result#)
	 result#)
      `(let [~@(apply concat non-constant-args)]
	 (run-pipeline []
	   :executor *current-executor*
	   ~@(map
	       (fn [arg]
		 `(read-merge
		    (fn [] ~arg)
		    (fn [args# val#]
		      (conj args#
			(if (fn? val#)
			  (transform-fn val#)
			  val#)))))
	       (map first non-constant-args))
	   (fn [[~@(->> non-constant-args (map first))]]
	     (debug-print
	       ~(str (first expr))
	       ~@(map #(if-let [x (first %)] x (str (second %))) args))
	     (let [result# (~(first expr) ~@(map #(if-let [x (first %)] x (second %)) args))]
	       (debug-print
		 ~(str (first expr))
		 ~@(map #(if-let [x (first %)] x (str (second %))) args) "=" result#)
	       result#)))))))

(defn transform-if [[_ predicate true-clause false-clause]]
  `(run-pipeline ~predicate
     :executor *current-executor*
     (fn [predicate#]
       (if predicate#
	 ~true-clause
	 ~@(when false-clause [false-clause])))))

(defn transform-lazy-seq [[_ _ f]]
  `(new clojure.lang.LazySeq (fn [] (await-result (~f)))))

(defn transform-new [[_ class-name & args :as expr]]
  (if (= (resolve class-name) clojure.lang.LazySeq)
    (transform-lazy-seq expr)
    `(run-pipeline []
       :executor *current-executor*
       ~@(map
	   (fn [arg] `(read-merge (constantly ~arg) conj))
	   args)
       ~(let [arg-syms (take (count args) (repeatedly gensym))]
	  `(fn [[~@arg-syms]]
	     (new ~class-name ~@arg-syms))))))

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

(def special-handlers
  {'if transform-if
   'throw transform-throw
   'try transform-try
   'new transform-new
   'task #(transform-task (rest %))})

(defn async [body]
  (let [body (->> body
	       (prewalk partial-macroexpand)
	       (auto-force 'read-channel)
	       tag-exprs)
	body (binding [*special-walk-handlers* special-handlers
		       *final-walk* true]
	       (realize
		 (walk-exprs
		   #(if (valid-expr? %)
		      (transform-expr %)
		      %)
		   body)))]
    `(run-pipeline nil
       (fn [_#]
	 ~@body))))

