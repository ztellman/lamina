;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.stats
  (:use
    [clojure test]
    [lamina stats core]
    [lamina.test utils])
  (:require
    [lamina.time :as t]))

;;;

(defn run-stats-test [f values period window]
  (let [q (t/non-realtime-task-queue)
        ch (channel)
        ch* (f
              {:period period
               :window window
               :task-queue q}
              ch)]
    (doseq [vs values]
      (apply enqueue ch vs)
      (t/advance q))
    (channel->seq ch*)))

(deftest test-average
  )

;;;

(deftest ^:benchmark benchmark-rate
  )

(deftest ^:benchmark benchmark-average
  )

(deftest ^:benchmark benchmark-variance
  )

(deftest ^:benchmark benchmark-outliers
  )
