;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.expr.tag
  (:use
    [clojure walk]
    [lamina.core.expr utils walk]))

(defn add-meta [x metadata]
  (if (instance? clojure.lang.IMeta x)
    (with-meta x (merge (meta x) metadata))
    x))

(defn strip-forces [x]
  (->> x
    (partition 2)
    (reduce
     (fn [[forced exprs] [variable value]]
       (let [forced? (first= value 'force)]
	 (when (and forced? (not= 2 (count value)))
	   (throw (Exception. (str value " must have only two arguments."))))
	 [(if forced? (conj forced variable) forced)
	  (conj exprs
	    [variable (add-meta (if forced? (second value) value) {:dependencies forced})])]))
     [[] []])
    second
    (apply concat)
    vec))

(defn tag-dependencies [x]
  (postwalk
    (fn [x]
      (if-not (first= x 'let 'let* 'binding)
	x
	(list* (first x) (strip-forces (second x)) (drop 2 x))))
    x))

(defn exprs->let-form [x]
  (walk-exprs
    (fn [x]
      (if (not (seq? x))
	x
	(let [const-args (constant-elements x)]
	  (if (> 2 (count (filter false? const-args)))
	    x
	    (let [args (rest
			 (map vector
			   const-args
			   (map #(if %1 %2 (gensym "arg")) const-args x)
			   x))]
	      `(let ~(->> args (filter (complement first)) (map rest) (apply concat) vec)
		 (~(first x) ~@(map second args))))))))
    x))

(defn do-forms->let-form [x]
  (postwalk
    (fn [x]
      (if-not (first= x 'do)
	x
	(let [binding-form (map vector (repeatedly #(gensym "do")) (rest x))]
	  `(let ~(vec (apply concat binding-form)) ~(-> binding-form last first)))))
    x))

(defn expand-do-forms [x]
  (postwalk
    (fn [x]
      (cond
	(first= x 'let 'let* 'fn 'fn* 'binding)
	(transform-special-form-bodies (fn [body] `((do ~@body))) x)

	(first= x 'try)
	(list* (first x)
	  (list* 'do (take-while #(not (first= % 'catch 'finally)) (rest x)))
	  (drop-while #(not (first= % 'catch 'finally)) (rest x)))

	(first= x 'finally)
	(list (first x) (list* 'do (rest x)))

	(first= x 'catch)
	(concat (take 3 x) [(list* 'do (drop 3 x))])

	:else
	x))
    x))

(defn tag-exprs [x]
  (-> x
    expand-do-forms
    do-forms->let-form
    exprs->let-form
    tag-dependencies))

(defn auto-force [sym x]
  (postwalk
    (fn [x]
      (if (first= x sym)
	(list 'force x)
	x))
    x))
