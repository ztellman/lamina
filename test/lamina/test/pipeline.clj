;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.pipeline
  (:use
    [lamina.core pipeline]
    [clojure test])
  (:require
    [lamina.core.node :as n]
    [lamina.core.result :as r]
    [criterium.core :as c]))

(defn defer [f]
  (fn [x]
    (let [r (r/result-channel)]
      (future (Thread/sleep 10) (r/success r (f x)))
      r)))

(defmacro repeated-pipeline [n f]
  `(pipeline ~@(repeat n f)))

;;;

(deftest test-simple-pipelines
  (dotimes [i 10]
    (eval
      `(do
         (is (= ~i @((repeated-pipeline ~i inc) 0)))
         (is (= ~i @((repeated-pipeline ~i (defer inc)) 0)))
         (is (= ~i @((repeated-pipeline ~i (fn [i#] (-> i# inc r/success-result r/success-result))) 0)))))))

(deftest test-restart
  (is (= 10 @(run-pipeline 0
               inc
               #(if (< % 10) (restart %) %))))
  (is (= 10 @(run-pipeline 0
               inc inc inc inc inc
               #(if (< % 10) (restart %) %)))))

(declare pipe-b)
(def pipe-a (pipeline inc #(if (< % 10) (redirect pipe-b %) %)))
(def pipe-b (pipeline inc #(if (< % 10) (redirect pipe-a %) %)))

(deftest test-redirect
  (is (= 10 @(pipe-a 0)))
  (is (= 10 @(pipe-b 0))))

;;;

(defmacro bench [n name & body]
  `(do
     (println "\n-----\n lamina.core.pipeline -" ~name "\n-----\n")
     (c/quick-bench
       (do
         (dotimes [_# (int ~n)]
           ~@body))
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-pipelines
  (let [f #(-> % inc inc inc inc inc)]
    (bench 1e6 "baseline raw-function"
      (f 0)))
  (let [f (apply comp (repeat 5 inc))]
    (bench 1e6 "baseline composition"
      (f 0)))
  (let [p (repeated-pipeline 5 inc)]
    (bench 1e6 "simple inc"
      (p 0)))
  (let [p (repeated-pipeline 5 r/success-result)]
    (bench 1e6 "simple success-result"
      (p 0)))
  (let [p (repeated-pipeline 5 #(-> % inc r/success-result r/success-result))]
    (bench 1e6 "nested success-result"
      (p 0)))
  (let [r (r/result-channel)
        _ (r/success r 1)
        p (repeated-pipeline 5 (fn [_] r))]
    (bench 1e6 "simple result-channel"
      (p 0)))
  (let [p (pipeline inc #(if (< % 10) (restart %) %))]
    (bench 1e6 "simple loop"
      (p 0)))
  (let [p (pipeline inc inc inc inc inc #(if (< % 10) (restart %) %))]
    (bench 1e6 "flattened loop"
      (p 0))))

(deftest ^:benchmark benchmark-nodes-and-pipelines
  (let [p (pipeline #(n/predicate-receive % nil nil nil) (fn [_] (restart)))
        n (n/node identity)]
    (p n)
    (bench 1e6 "receive loop"
      (n/propagate n 1 true))))
