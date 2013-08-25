;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.time
  (:use
    [lamina core time]
    [lamina.test.utils]
    [clojure.test]))

(deftest test-periodically
  (let [q (non-realtime-task-queue 0 false)
        cnt (atom 0)
        ch (periodically
             {:period 10, :task-queue q}
             #(swap! cnt inc))]

    (advance-until q 50)
    (is (= (range 1 6) (channel->seq ch)))

    (advance-until q 100)
    (is (= (range 6 11) (channel->seq ch)))))

(deftest test-defer-onto-queue
  (let [q (non-realtime-task-queue)
        ch (channel)
        ch* (->> ch
              (defer-onto-queue
                {:task-queue q
                 :timestamp :timestamp})
              (map* :timestamp))]

    (dotimes [i 10]
      (enqueue ch {:timestamp i}))

    (advance-until q 4)
    (is (= (range 0 5) (channel->seq ch*)))

    (advance-until q 100)
    (is (= (range 5 10) (channel->seq ch*))))

  (let [q (non-realtime-task-queue)
        ch (channel)
        ch* (->> ch
              (defer-onto-queue
                {:task-queue q
                 :timestamp :timestamp
                 :auto-advance? true})
              (map* :timestamp))]
    
    (doseq [i (range 0 5)]
      (enqueue ch {:timestamp i}))
    
    (is (= (range 0 5) (channel->seq ch*)))
    

    (doseq [i (range 5 10)]
      (enqueue ch {:timestamp i}))
    
    (is (= (range 5 10) (channel->seq ch*)))))

;;;

(deftest ^:benchmark benchmark-periodically
  (let [q (non-realtime-task-queue 0 false)
        ch (periodically {:period 10} (constantly 1) q)]
    (bench "non-realtime periodically"
      (advance q)
      @(read-channel ch))))

(deftest ^:benchmark benchmark-defer-onto-queue
  (let [q (non-realtime-task-queue 0 false)
        ch (channel)
        ch* (defer-onto-queue q identity ch)]
    (bench "defer-onto-queue"
      (enqueue ch 0)
      (advance q)
      @(read-channel ch*))))
