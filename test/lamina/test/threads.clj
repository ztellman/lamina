;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.threads
  (:use
    [clojure test])
  (:require
    [criterium.core :as c]
    [lamina.core.threads :as t]))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.test.threads -" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-threads
  (bench "scheduled-executor"
    (t/delay-invoke 100 (fn [])))
  (bench "cleanup-executor"
    (t/enqueue-cleanup (fn []))))

