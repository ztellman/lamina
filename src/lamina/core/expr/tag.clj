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
    [lamina.core.expr utils]))

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
	 (when forced?
	   (assert (= 2 (count x))))
	 [(if forced? (conj forced variable) forced)
	  (conj exprs
	    [variable (add-meta (if forced? (second value) value) {:depedencies forced})])]))
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

(defn transform-do-forms [x]
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
      (if-not (first= x 'let 'let* 'fn 'fn* 'binding)
	x
	(transform-special-form-bodies (fn [body] `((do ~@body))) x)))
    x))

(defn tag-exprs [x]
  (-> x
    expand-do-forms
    transform-do-forms
    tag-dependencies))
