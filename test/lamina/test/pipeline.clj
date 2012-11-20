;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.pipeline
  (:use
    [lamina core
     [executor :exclude (defer)]]
    [clojure test]
    [lamina.test utils]))

(defn defer [f]
  #(run-pipeline %
     (wait-stage 20)
     f))

(defn boom [_]
  (throw (Exception. "boom")))

(def exc (executor {:name :test-pipeline}))

(defmacro repeated-pipeline [n f]
  `(pipeline ~@(repeat n f)))

(defmacro repeated-executor-pipeline [n f]
  `(pipeline {:executor exc} ~@(repeat n f)))

(defmacro repeated-run-pipeline [n initial-val f]
  `(run-pipeline ~initial-val ~@(repeat n f)))

;;;

(deftest test-simple-pipelines
  (dotimes [i 10]
    (eval
      `(do
         (is (= ~i @((repeated-pipeline ~i inc) 0)))
         (is (= ~i @((repeated-pipeline ~i (defer inc)) 0)))
         (is (= ~i @((repeated-executor-pipeline ~i inc) 0)))
         (is (= ~i @((repeated-executor-pipeline ~i (defer inc)) 0)))
         (is (= ~i @((repeated-pipeline ~i (fn [i#] (-> i# inc success-result success-result))) 0)))))))

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

(def ^:dynamic a 1)

(deftest test-unwrap
  (is (= 1 (run-pipeline 0 {:unwrap? true} inc)))
  (is (= 1 @(run-pipeline 0 {:unwrap? true} (defer inc)))))

(deftest test-with-bindings
  (is (= 2 @(run-pipeline a
              inc)))
  (is (= 2 @(run-pipeline a
              (wait-stage 20)
              inc)))
  (is (= 2 @(binding [a 3]
              (run-pipeline nil
                (wait-stage 20)
                (constantly a)
                inc))))
  (is (= 4 @(binding [a 3]
              (run-pipeline a
                (wait-stage 20)
                inc))))
  (is (= 4 @(binding [a 3]
              (run-pipeline nil
                {:with-bindings? true}
                (wait-stage 20)
                (constantly a)
                inc)))))

(deftest test-error-handler
  (is (thrown? Exception @(run-pipeline nil
                            {:error-handler nil}
                            boom)))
  (is (thrown? Exception @(run-pipeline nil
                            {:error-handler (fn [_])}
                            boom)))
  (is (thrown? Exception @(run-pipeline nil
                            {:timeout 1}
                            (wait-stage 100))))
  (is (= 1 @(run-pipeline nil
              {:error-handler (fn [_] (complete 1))}
              boom)))
  (is (thrown? Exception @(run-pipeline nil
                            {:timeout 1
                             :error-handler (fn [_] (complete 2))}
                            (wait-stage 100))))
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
  (let [p (repeated-pipeline 5 success-result)]
    (bench "simple success-result"
      (p 0)))
  (let [p (repeated-pipeline 5 #(-> % inc success-result success-result))]
    (bench "nested success-result"
      (p 0)))
  (let [r (result-channel)
        _ (success r 1)
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
