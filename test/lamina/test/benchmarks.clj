;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.benchmarks
  (:use
    [clojure test])
  (:require
    [criterium.core :as cr]
    [lamina.core.channel :as c]
    [lamina.core.pipeline :as p]
    [lamina.core.threads :as t]))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (cr/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmarks
  (let [p (p/pipeline c/read-channel (constantly (p/restart)))
        ch (c/channel)]
    (p ch)
    (bench "read-channel pipeline loop"
      (c/enqueue ch :msg))))
