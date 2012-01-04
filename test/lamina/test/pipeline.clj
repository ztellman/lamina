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
    [lamina.core.result :as r]
    [criterium.core :as c]))

(defn defer [f]
  (fn [x]
    (let [r (r/result-channel)]
      (future (Thread/sleep 10) (r/success r (f x)))
      r)))

(deftest test-simple-pipelines
  (is (= 3 @(run-pipeline 1 inc inc)))
  (is (= 3 @(run-pipeline 1 (defer inc) (defer inc)))))

;;;

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.pipeline -" ~name "\n-----\n")
     (c/quick-bench
       (do
         ;;~@body
         (dotimes [_# (int 1e3)]
           ~@body))
       :reduce-with #(and %1 %2))))

(defmacro repeated-pipeline [n f]
  `(pipeline ~@(repeat n f)))

(deftest ^:benchmark benchmark-pipelines
  (let [f (apply comp (repeat 5 inc))]
    (bench "baseline composition"
      (f 0)))
  (let [p (repeated-pipeline 5 inc)]
    (bench "simple inc"
      (p 0)))
  (let [p (repeated-pipeline 5 r/success-result)]
    (bench "simple success-result"
      (p 0)))
  (let [p (repeated-pipeline 5 #(r/success-result (r/success-result %)))]
    (bench "nested success-result"
      (p 0))))
