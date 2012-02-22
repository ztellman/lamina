;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats
  (:use
    [lamina.core]
    [lamina.core.channel :only (bridge-join)])
  (:require
    [lamina.time :as t])
  (:import
    [com.yammer.metrics.stats
     EWMA
     ExponentiallyDecayingSample
     Sample
     Snapshot]
    [com.yammer.metrics.core
     Histogram]
    [java.util.concurrent
     TimeUnit]
    [java.util.concurrent.atomic
     AtomicLong]))

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
     (let [alpha (- 1 (Math/exp (/ (- interval) window)))
           emwa (EWMA. alpha interval TimeUnit/MILLISECONDS)
           cnt (AtomicLong. 0)
           ch* (channel)]
       (bridge-join ch "average"
         #(do
            (.incrementAndGet cnt)
            (.update emwa (long %)))
         ch*)
       (siphon
         (periodically interval
           (fn []
             (let [rate (.getAndSet cnt 0)]
               (.tick emwa)
               (if (zero? rate)
                 0
                 (/ (* interval (.rate emwa TimeUnit/MILLISECONDS)) rate)))))
         ch*)
       ch*)))

(defn quantiles
  ([ch]
     (quantiles (t/seconds 5) ch))
  ([interval ch]
     (quantiles interval (t/minutes 5) ch))
  ([interval window ch]
     (let [sample (ExponentiallyDecayingSample. 1028 0.015)
           ch* (channel)]
       (bridge-join ch "quantiles" #(.update sample (long %)) ch*)
       (siphon
         (periodically interval
           (fn []
             (let [snapshot (.getSnapshot sample)]
               {50 (.getMedian snapshot)
                75 (.get75thPercentile snapshot)
                95 (.get95thPercentile snapshot)
                98 (.get98thPercentile snapshot)
                99 (.get99thPercentile snapshot)
                999 (.get999thPercentile snapshot)})))
         ch*)
       ch*)))



