;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.executor
  (:use
    [lamina core executor]
    [lamina.test utils]
    [clojure test])
  (:import
    [java.util.concurrent
     CountDownLatch
     Executor
     Executors]))

(def simple-executor (Executors/newSingleThreadExecutor))

(def lamina-executor (executor :name "test"))

(def n 1e3)

(deftest ^:benchmark benchmark-executor
  (bench "baseline executor countdown"
    (let [latch (CountDownLatch. (int n))]
      (dotimes [_ n]
        (.execute ^Executor simple-executor #(.countDown latch)))
      (.await latch)))
  (bench "executor countdown"
    (let [latch (CountDownLatch. (int n))]
      (dotimes [_ n]
        (execute lamina-executor #(.countDown latch)))
      (.await latch))))
