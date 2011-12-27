;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.lock
  (:use
    [lamina.core lock]
    [clojure test])
  (:require
    [criterium.core :as c]))

(deftest test-lock-reentrancy
  (let [l (asymmetric-reentrant-lock false)]
    (is (= 1 (->> 1
               (with-non-exclusive-reentrant-lock l)
               (with-exclusive-reentrant-lock l)
               (with-non-exclusive-reentrant-lock l)
               (with-exclusive-reentrant-lock l))))))

;;;

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.lock -" ~name "\n-----\n")
     (c/bench
       (do
         ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-locks
  (let [lock (asymmetric-lock false)]
    (bench "non-exclusive"
      (with-non-exclusive-lock lock 1))
    (bench "exclusive"
      (with-exclusive-lock lock 1))
    (bench "non-exclusive*"
      (with-non-exclusive-lock* lock 1))
    (bench "exclusive*"
      (with-exclusive-lock* lock 1)))
  (let [lock (asymmetric-reentrant-lock false)]
    (bench "non-exclusive-reentrant"
      (with-non-exclusive-reentrant-lock lock 1))
    (bench "multi non-exclusive-reentrant"
      (->> 1
        (with-non-exclusive-reentrant-lock lock)
        (with-non-exclusive-reentrant-lock lock)))
    (bench "exclusive-reentrant"
      (with-exclusive-reentrant-lock lock 1))
    (bench "multi exclusive-reentrant"
      (->> 1
        (with-non-exclusive-reentrant-lock lock)
        (with-non-exclusive-reentrant-lock lock)))))

