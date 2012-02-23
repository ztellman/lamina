;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats
  (:use
    [lamina core api]
    [lamina.stats.utils :only (update)])
  (:require
    [lamina.time :as t]
    [lamina.stats.moving-average :as avg])
  (:import
    [com.yammer.metrics.stats
     ExponentiallyDecayingSample
     Snapshot]))

(set! *warn-on-reflection* true)

(defn sum
  ([ch]
     (sum 1000 ch))
  ([interval ch]
     (let [ch* (channel)
           cnt (AtomicLong. 0)]
       (bridge-join ch "sum" #(.addAndGet cnt %) ch*)
       (siphon (periodically interval #(.getAndSet cnt 0)) ch*)
       ch*)))

(defn rate
  ([ch]
     (rate 1000 ch))
  ([interval ch]
     (->> ch
       (map* (fn [_] 1))
       (sum interval))))

(defn average
  ([ch]
     (average (t/seconds 5) ch))
  ([interval ch]
     (average interval (t/minutes 5) ch))
  ([interval window ch]
     (let [avg (avg/moving-average interval window)
           ch* (channel)]
       (bridge-join ch "average" #(update avg (long %)) ch*)
       (siphon
         (periodically interval #(deref avg))
         ch*)
       ch*)))

(defn quantiles
  ([ch]
     (quantiles (t/seconds 5) ch))
  ([interval ch]
     (quantiles interval [50 75 95 99 99.9] ch))
  ([interval quantiles ch]
     (let [sample (ExponentiallyDecayingSample. 1028 0.015)
           ch* (channel)
           scaled-quantiles (map #(/ % 100) quantiles)]
       (bridge-join ch "quantiles" #(.update sample (long %)) ch*)
       (siphon
         (periodically interval
           (fn []
             (let [snapshot (.getSnapshot sample)]
               (zipmap
                 quantiles
                 (map #(.getValue snapshot %) scaled-quantiles)))))
         ch*)
       ch*)))



