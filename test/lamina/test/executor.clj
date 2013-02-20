;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.executor
  (:use
    [lamina core executor trace]
    [lamina.test utils]
    [clojure test])
  (:import
    [java.util.concurrent
     CountDownLatch
     Executor
     Executors]))

;;;

(def n 1e3)

(defn benchmark-active-probe [description probe]
  (let [nm (gensym "name")
        x (executor {:name nm :max-thread-count 1})
        p (probe-channel [nm probe])
        f (instrument #(.countDown ^CountDownLatch %)
            {:executor x
             :name nm})]
    (receive-all p (fn [_]))
    (bench description
      (let [latch (CountDownLatch. (int n))]
        (dotimes [_ n]
          (f latch))
        (.await latch)))))

(deftest ^:stress stress-test-task
  )

(deftest ^:benchmark benchmark-executor
  #_(let [^Executor x (Executors/newSingleThreadExecutor)]
    (bench "baseline executor countdown"
      (let [latch (CountDownLatch. (int n))]
        (dotimes [_ n]
          (.execute x #(.countDown latch)))
        (.await latch))))
  (let [x (executor {:name "test" :max-thread-count 1 :min-thread-count 1})
        f (instrument #(.countDown ^CountDownLatch %)
            {:executor x
             :name :foo})]
    (bench "executor countdown"
      (let [latch (CountDownLatch. (int n))]
        (dotimes [_ n]
          (f latch))
        (.await latch))))

  #_(bench "parallel task countdown"
    (let [latch (CountDownLatch. (int n))]
      (dotimes [_ n]
        (task (.countDown latch)))
      (.await latch)))

  #_(bench "serial task countdown"
    (let [latch (CountDownLatch. (int n))]
      (dotimes [_ n]
        @(task (.countDown latch)))
      (.await latch)))
  
  #_(benchmark-active-probe "executor countdown with return probe" :return)
  #_(benchmark-active-probe "executor countdown with enter probe" :enter))
