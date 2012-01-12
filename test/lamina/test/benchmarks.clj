;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.benchmarks
  (:use
    [clojure test]
    [lamina core])
  (:require
    [criterium.core :as c]))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(defn map-seq [ch f]
  (iterate #(map* f %) ch))

(deftest ^:benchmark benchmarks
  (let [p (pipeline
            read-channel
            (constantly (restart)))
        ch (channel)]
    (p ch)
    (bench "read-channel pipeline loop"
      (enqueue ch :msg)))
  (let [ch (channel)]
    (->
      (take 1e3 (map-seq ch inc))
      last
      (receive-all (fn [_])))
    (bench "map* chain"
      (enqueue ch 1))))
