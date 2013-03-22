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
    [lamina.test utils]
    [lamina.core channel utils]
    [lamina.core.operators :only (channel->seq)])
  (:require
    [lamina.core.pipeline :as p]))

;;;

(defn capture []
  (let [a (atom [])]
    [a
     #(do
        (swap! a conj %)
        true)]))

;;;

(deftest test-map*
  (let [[v callback] (capture)
        a (channel 0 1 2)
        b (map* inc a)]
    (receive-all b callback)
    (enqueue a 3)
    (enqueue b 4)
    (is (= (range 1 6) @v))))

(deftest test-filter*
  (let [[v callback] (capture)
        a (channel 0 1 2)
        b (->> a (map* inc) (filter* even?))]
    (receive-all b callback)
    (enqueue a 3)
    (enqueue a 4)
    (is (= [2 4] @v))))

(defn run-split-test [operator channel-fn]
  (let [a (channel-fn 0 1 2)
        b (->> a operator (map* inc))
        c (->> a operator (filter* even?))
        d (->> b operator (filter* even?))]
    (is (= [0 1 2] (channel->seq a)))
    (is (= [1 2 3] (channel->seq b)))
    (is (= [0 2] (channel->seq c)))
    (is (= [2] (channel->seq d)))))

(deftest test-fork
  (run-split-test fork channel)
  (run-split-test fork closed-channel)
  (let [a (channel* :permanent? true)
        b (fork a)]
    (close b)
    (is (closed? b))
    (is (not (closed? a)))))

(deftest test-tap
  (run-split-test tap channel)
  (run-split-test tap closed-channel)
  (let [a (channel)
        b (map* identity a)
        c (tap a)]
    (close b)
    (is (closed? a))
    (is (closed? c))))

(deftest test-to-string
  (is (= "<== []" (str (closed-channel)))))

;;;

(deftest ^:benchmark benchmark-channels
  (bench "create channel"
    (channel))
  (bench "create and map*"
    (->> (channel) (map* inc)))
  (bench "create and fork"
    (->> (channel) fork))
  (let [ch (channel)]
    (receive-all ch (fn [_]))
    (bench "simple-enqueue"
      (enqueue ch :msg)))
  (bench "create and enqueue"
    (let [ch (channel)]
      (enqueue ch :msg)))
  (bench "create, read, and enqueue"
    (let [ch (channel)]
      (read-channel ch)
      (enqueue ch :msg)))
  (bench "create, enqueue, and read"
    (let [ch (channel)]
      (enqueue ch :msg)
      (read-channel ch)))
  (bench "create, receive-all, and enqueue"
    (let [ch (channel)]
      (receive-all ch (fn [_]))
      (enqueue ch :msg)))
  (bench "create, enqueue, and receive-all"
    (let [ch (channel)]
      (enqueue ch :msg)
      (receive-all ch (fn [_])))))

(deftest ^:benchmark benchmark-queues
  (let [ch (channel)]
    (bench "enqueue 1 message"
      (enqueue ch 1)
      (channel->seq ch))
    (bench "enqueue 1e3 messages"
      (dotimes [_ 1e3]
        (enqueue ch 1))
      (channel->seq ch))
    #_(bench "enqueue 1e6"
        (dotimes [_ 1e6]
          (enqueue ch 1))
        (channel->seq ch))))

(deftest ^:benchmark benchmark-join
  (bench "join 1"
    (let [ch (channel)]
      (join ch (channel))))
  (bench "join 1e3"
    (let [ch (channel)]
      (dotimes [_ 1e3]
        (join ch (channel)))))
  (let [ch (channel)]
    (ground (join ch (channel)))
    (bench "enqueue to 1"
      (enqueue ch 1)))
  (let [ch (channel)]
    (dotimes [_ 1e3]
      (ground (join ch (channel))))
    (bench "enqueue to 1e3"
      (enqueue ch 1)))
  (let [ch (channel)]
    (dotimes [_ 1e5]
      (ground (join ch (channel))))
    (bench "enqueue to 1e5"
      (enqueue ch 1))))
