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
    [lamina.core.channel :only (mimic)]
    [lamina.stats.utils :only (update)])
  (:require
    [lamina.time :as t]
    [lamina.stats.moving-average :as avg]
    [lamina.stats.variance :as var])
  (:import
    [java.util.concurrent.atomic
     AtomicLong]
    [com.yammer.metrics.stats
     ExponentiallyDecayingSample
     Snapshot]))

(defn sum
  "Returns a channel that will periodically emit the sum of all messages emitted by the source
   channel over the last 'interval' milliseconds, with a default of 1000.

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
  "Returns a channel that will periodically emit the number of messages emitted by the source
   channel over the last 'interval' milliseconds, with a default of 1000."
  ([ch]
     (rate 1000 ch))
  ([interval ch]
     (->> ch
       (map* (constantly 1))
       (sum interval))))

(defn average
  "Returns a channel that will periodically emit the moving average over all messages emitted by
   the source channel every 'interval' milliseconds, defaulting to once every five seconds.  This
   moving average is exponentially weighted to the last 'window' milliseconds, defaulting to the
   last five minutes."
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
  "Returns a channel that will periodically emit a map of quantile values every 'interval'
   millseconds, which represent the statistical distribution of values emitted by the source
   channel over the last five minutes.

   The map will be of quantile onto quantile value, so for a uniform distribution of values from
   1..1000, it would emit

     {50 500, 75 750, 95 950, 99 990, 99.9 999}

   By default, the above quantiles will be used, these can be specified as a sequence of quantiles
   of the form [50 98 99.9 99.99]."
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

(defn variance
  "Returns a channel that will periodically emit the variance of all values emitted by the source
   channel every 'interval' milliseconds."
  ([ch]
     (variance (t/seconds 5) ch))
  ([interval ch]
     (let [vr (atom (var/create-variance))
           ch* (channel)]
       (bridge-join ch "variance" #(swap! vr update (long %)) ch*)
       (siphon
         (periodically interval #(var/variance @vr))
         ch*)
       ch*)))

(defn outliers
  "Returns a channel that will emit outliers from the source channel, as measured by the standard
   deviations from the mean value of (facet msg).  Outlier status is determined by
   'deviation-predicate', which is given the standard deviations from the mean, and returns true
   or false.  By default, it will return true for any value where the absolute value is greater
   than three.

   For instance, to monitor function calls that take an unusually long or short time via a
   'return' probe:

     (outliers :duration (probe-channel :name:return))

   To only receive outliers that are longer than the mean, define a custom deviation-predicate:

     (outliers :duration (lamina.time/minutes 5) #(< % 3) (probe-channel :name:return))

   'window' describes the window of the moving average, which defaults to five minutes.  This can
    be used to adjust the responsiveness to long-term changes to the mean."
  ([facet ch]
     (outliers facet (t/minutes 5) ch))
  ([facet window ch]
     (outliers facet window #(< 3 (Math/abs (double %))) ch))
  ([facet window deviation-predicate ch]
     (let [f (atom nil)
           avg (avg/moving-average (t/seconds 5) window)
           vr (atom (var/create-variance))
           ch* (mimic ch)

           predicate-updater
           (periodically 5000
             (fn []
               (let [mean @avg
                     std-dev (var/std-dev @vr)]
                 (when-not (zero? std-dev)
                   #(deviation-predicate
                      (/ (- (facet %) mean) std-dev))))))]

       ;; periodically update the outlier predicate
       (receive-all predicate-updater #(reset! f %))
       (on-drained ch #(close predicate-updater))

       (bridge-join ch "outliers"
         (fn [msg]
           (when-let [val (facet msg)]
             (update avg val)
             (swap! vr update val)
             (when-let [f @f]
               (prn val (f val))
               (when (f val)
                 (enqueue ch* msg)))))
         ch*)
       ch*)))
