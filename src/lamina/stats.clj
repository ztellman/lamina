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
    [java.util.concurrent.atomic
     AtomicLong]
    [com.yammer.metrics.stats
     ExponentiallyDecayingSample
     Snapshot]))

(defn sum
  "Returns a channel that will periodically emit the sum of all messages emitted by the source channel over the
   last 'interval' milliseconds.

   It is assumed that all numbers emitted by the source channel are integral values."
  ([ch]
     (sum 1000 ch))
  ([interval ch]
     (let [ch* (channel)
           cnt (AtomicLong. 0)]
       (bridge-join ch "sum" #(.addAndGet cnt (long %)) ch*)
       (siphon (periodically interval #(.getAndSet cnt 0)) ch*)
       ch*)))

(defn rate
  "Returns a channel that will periodically emit the number of messages emitted by the source channel over the
   last 'interval' milliseconds."
  ([ch]
     (rate 1000 ch))
  ([interval ch]
     (->> ch
       (map* (fn [_] 1))
       (sum interval))))

(defn average
  "Returns a channel that will periodically emit the moving average over all messages emitted by the source
   channel every 'interval' milliseconds, defaulting to once every five seconds.  This moving average is
   exponentially weighted to the last 'window' milliseconds, defaulting to the last five minutes.

   It is assumed that all numbers emitted by the source channel are integral values."
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
  "Returns a channel that will periodically emit a map of quantile values every 'interval' millseconds, which
   represent the statistical distribution of values emitted by the source channel over the last five minutes.

   The map will be of quantile onto quantile value, so for a uniform distribution of values from 1..1000, it
   would emit

     {50 500, 75 750, 95 950, 99 990, 99.9 999}

   By default, the above quantiles will be used, these can be specified as a sequence of quantiles of the form
   [50 98 99.9 99.99].

   It is assumed that all values emitted by the source channel are integral values."
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



