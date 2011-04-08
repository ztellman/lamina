;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.expr.utils
  (:use
    clojure.pprint
    [lamina.core pipeline channel]
    clojure.walk)
  (:import [lamina.core.pipeline ResultChannel]))

(defn first= [expr & symbols]
  (and
    (seq? expr)
    (symbol? (first expr))
    (some #{(symbol (name (first expr)))} symbols)))

(defn print-vals [& args]
  (doseq [a args]
    (pprint a))
  (last args))

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

(def special-forms
  '(let if do let let* fn fn* quote var throw loop loop* recur try catch finally new def task))

(defn constant? [x]
  (or
    (number? x)
    (string? x)
    (nil? x)
    (keyword? x)
    (and (symbol? x) (class? (resolve x)))))

(defn constant-elements [x]
  (if (first= x '.)
    (list* true (constant? (second x)) true (map constant? (drop 3 x)))
    (list* true (map constant? (rest x)))))

;;;

(defn split-special-form [x]
  (let [x* (partition 2 1 x)
	f #(not (vector? (first %)))]
    (if-not (some vector? x)
      [(take-while symbol? x) (drop-while symbol? x)]
      [(cons (first x) (->> x* (take-while f) (map second)))
       (->> x* (drop-while f) (map second))])))

(defn realize [x]
  (postwalk identity x))

(defn transform-special-form-bodies [f x]
  (let [[prefix body] (split-special-form x)]
    (if (vector? (last prefix))
      (concat prefix (f body))
      (concat
	(take-while symbol? prefix)
	(map
	  #(transform-special-form-bodies f %)
	  (concat (drop-while symbol? prefix) body))))))

(defn wrap-with-dependencies [expr expr*]
  (let [dependencies (-> expr meta :dependencies)]
    (if (empty? dependencies)
      expr*
      `(run-pipeline nil
	 ~@(map
	     (fn [v] `(read-merge (constantly ~v) (constantly nil)))
	     dependencies)
	 (fn [_#]
	   ~expr*)))))

;;;

(defn- accumulate-result-channels [accum x]
  (cond
    (result-channel? x) (conj accum x)
    (or (sequential? x) (map? x)) (reduce accumulate-result-channels accum (seq x))
    :else accum))

(defn compact [x]
  (let [results (accumulate-result-channels [] x)]
    (if (empty? results)
      x
      (apply run-pipeline nil
	(conj
	  (vec (map constantly results))
	  (fn [_]
	    (compact (postwalk #(if (result-channel? %) @% %) x))))))))
