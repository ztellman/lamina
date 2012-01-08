;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.channel
  (:use
    [clojure test]
    [lamina.core channel])
  (:require
    [lamina.core.pipeline :as p]
    [criterium.core :as c]))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.channel -" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-channels
  (bench "create channel"
    (channel))
  (bench "create and map*"
    (->> (channel) (map* inc)))
  (let [ch (channel)]
    (receive-all ch (fn [_]))
    (bench "simple-enqueue"
      (enqueue ch :msg)))
  (bench "create and enqueue"
    (let [ch (channel)]
      (enqueue ch :msg)))
  (bench "create, multi-receive, and multi-enqueue"
    (let [ch (channel)]
      (receive ch (fn [_]))
      (receive ch (fn [_]))
      (enqueue ch :msg)
      (enqueue ch :msg)))
  (bench "create, multi-enqueue, and multi-receive"
    (let [ch (channel)]
      (enqueue ch :msg)
      (enqueue ch :msg)
      (receive ch (fn [_]))
      (receive ch (fn [_]))))
  (bench "create, receive, and enqueue"
    (let [ch (channel)]
      (receive ch (fn [_]))
      (enqueue ch :msg)))
  (bench "create, receive-all, and enqueue"
    (let [ch (channel)]
      (receive-all ch (fn [_]))
      (enqueue ch :msg)))
  (bench "create, enqueue, and receive"
    (let [ch (channel)]
      (enqueue ch :msg)
      (receive ch (fn [_]))))
  (bench "create, enqueue, and receive-all"
    (let [ch (channel)]
      (enqueue ch :msg)
      (receive-all ch (fn [_]))))
  (let [p (p/pipeline read-channel (constantly (p/restart)))
        ch (channel)]
    (p ch)
    (bench "read-channel pipeline loop"
      (enqueue ch :msg))))
