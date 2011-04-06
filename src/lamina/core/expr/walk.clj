;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.expr.walk
  (:use
    [lamina.core.expr utils task]
    [lamina.core pipeline]
    clojure.walk))

(declare walk-exprs)

(def *recur-point* nil)

(defn walk-bindings [f bindings]
  (vec
    (interleave
      (->> bindings (partition 2) (map first))
      (->> bindings (partition 2) (map second) (map #(walk-exprs f %))))))

(defn walk-special-form [f x]
  (let [[x xs] (split-special-form x)
	binding-form? (first= x 'let 'let* 'loop 'loop*)]
    (concat
      (butlast x)
      [(if binding-form?
	 (walk-bindings f (last x))
	 (last x))]
      (f (map #(walk-exprs f %) xs)))))

(defn walk-task-form [f x]
  (transform-task
    (map #(walk-exprs f %) (rest x))))

(defn walk-fn-form [f x]
  (let [pipeline-sym (gensym "fn")
	f* #(walk-exprs f %)]
    (binding [*recur-point* pipeline-sym]
      (realize
	`(let [~pipeline-sym (atom nil)]
	   (reset! ~pipeline-sym
	     (pipeline
	       (fn [x#]
		 (apply
		   ~(transform-special-form-bodies #(map f* %) x)
		   x#))))
	   (fn [~'& args#]
	     ((deref ~pipeline-sym) args#)))))))

(defn walk-loop-form [f x]
  (let [pipeline-sym (gensym "loop")
	f* #(walk-exprs f %)]
    (binding [*recur-point* pipeline-sym]
      (realize
	`(let [~pipeline-sym (atom nil)]
	   (reset! ~pipeline-sym
	     (pipeline
	       (fn [~(vec (->> x second (partition 2) (map first)))]
		 ~@(map #(walk-exprs f %) (drop 2 x)))))
	   ((deref ~pipeline-sym)
	    ~(->> x second (partition 2) (map second) (map #(walk-exprs f %)) vec)))))))

(defn walk-exprs* [f x]
  (let [f* #(walk-exprs f %)]
    (realize
      (cond
	(vector? x) (f (list* 'vector (map f* x)))
	(set? x) (f (list* 'set (map f* x)))
	(map? x) (f (list* 'hash-map (map f* (apply concat x))))
	(sequential? x) (cond
			  (first= x 'fn 'fn*)
			  (walk-fn-form f x)

			  (first= x 'let 'let*)
			  (walk-special-form f x)

			  (first= x 'loop 'loop*)
			  (walk-loop-form f x)

			  (first= x 'chunk-append)
			  `(chunk-append
			     (await-result ~(second x))
			     (await-result ~(->> x rest second (walk-exprs f))))

			  (first= x 'task)
			  (walk-task-form f x)
			  
			  (first= x 'recur)
			  `(redirect (deref ~*recur-point*) [~@(map f* (rest x))])

			  :else
			  (f (map f* x)))
	:else (f x)))))

(defn walk-exprs [f x]
  (if (first= x 'read-channel)
    (walk-exprs* f (list 'force x))
    (walk-exprs* f x)))
