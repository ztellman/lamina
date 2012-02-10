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
    [clojure test]
    [lamina.test utils])
  (:require
    [lamina.core.node :as n]
    [lamina.core.result :as r]))

(defn defer [f]
  (fn [x]
    (let [r (r/result-channel)]
      (future (Thread/sleep 10) (r/success r (f x)))
      r)))

(defn boom [_]
  (throw (Exception. "boom")))

(defmacro repeated-pipeline [n f]
  `(pipeline ~@(repeat n f)))

(defmacro repeated-run-pipeline [n initial-val f]
  `(run-pipeline ~initial-val ~@(repeat n f)))

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

(deftest test-error-handler
  (is (thrown? Exception @(run-pipeline nil
                            {:error-handler nil}
                            boom)))
  (is (thrown? Exception @(run-pipeline nil
                            {:error-handler (fn [_])}
                            boom)))
  (is (= 1 @(run-pipeline nil
              {:error-handler (fn [_] (complete 1))}
              boom)))
  (is (= 1 @(run-pipeline nil
              {:error-handler (pipeline
                                (wait-stage 100)
                                (fn [_] (complete 1)))}
              boom))))

;;;

(deftest ^:benchmark benchmark-pipelines
  (let [f #(-> % inc inc inc inc inc)]
    (bench "baseline raw-function"
      (f 0)))
  (let [f (apply comp (repeat 5 inc))]
    (bench "baseline composition"
      (f 0)))
  (let [p (repeated-pipeline 5 inc)]
    (bench "simple inc"
      (p 0)))
  (bench "run-pipeline inc"
    (repeated-run-pipeline 0 5 inc))
  (let [p (repeated-pipeline 5 r/success-result)]
    (bench "simple success-result"
      (p 0)))
  (let [p (repeated-pipeline 5 #(-> % inc r/success-result r/success-result))]
    (bench "nested success-result"
      (p 0)))
  (let [r (r/result-channel)
        _ (r/success r 1)
        p (repeated-pipeline 5 (fn [_] r))]
    (bench "simple result-channel"
      (p 0)))

  ;;;
  (let [p (pipeline inc #(if (< % 10) (restart %) %))]
    (bench "simple loop"
      (p 0)))
  (let [p (pipeline inc inc inc inc inc #(if (< % 10) (restart %) %))]
    (bench "flattened loop"
      (p 0))))
