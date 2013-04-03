;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.stats.sample
  (:use
    [potemkin]
    [lamina.stats.utils]
    [lamina.core.utils])
  (:require
    [lamina.core.lock :as l]
    [lamina.time :as t])
  (:import
    [lamina.stats.utils
     IUpdatable]
    [java.util.concurrent
     ConcurrentSkipListMap]
    [java.util.concurrent.atomic
     AtomicLong
     AtomicReferenceArray]))

(enable-unchecked-math)

;; implementations strongly based on those in Code Hale's metrics-core library
;; which are in turn based on:
;; http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf

(definterface+ IExponentiallyDecayingSampler
  (rescale [_ next]))

(defn priority [alpha elapsed]
  (/
    (double
      (Math/exp
        (* (double alpha) (double elapsed))))
    (double
      (rand))))

(def rescale-interval (t/hours 1))

(deftype+ ExponentiallyDecayingSampler
  [^ConcurrentSkipListMap samples
   ^AtomicLong counter
   ^AtomicLong next-rescale
   ^{:volatile-mutable true, :tag long} start-time
   ^double alpha
   lock
   task-queue
   ^long sample-size]

  clojure.lang.IDeref
  (deref [_]
    (l/with-lock lock
      (vals samples)))
  
  IUpdatable
  (update [this val]
    (let [now (t/now task-queue)]
      (if (>= now (.get next-rescale))

        ;; we need to rescale
        (do
          (rescale this (.get next-rescale))
          (update this val))

        (let [elapsed (- now start-time)
              pr (priority alpha elapsed)]
          (l/with-lock lock
            (if (< (.incrementAndGet counter) sample-size)
              
              ;; we don't have our full sample size, add everything
              (.put samples pr val)
              
              ;; check to see if we should displace an existing sample
              (let [frst (.firstKey samples)]
                (when (< frst pr)
                  (when-not (.putIfAbsent samples pr val)
                    (loop [frst frst]
                      (when-not (.remove samples frst)
                        (recur (.firstKey samples)))))))))))))

  IExponentiallyDecayingSampler
  (rescale [_ next]
    (when (.compareAndSet next-rescale next (+ (t/now task-queue) (long rescale-interval)))
      (l/with-exclusive-lock lock
        (let [prev-start-time start-time
              _ (set! start-time (long (t/now task-queue)))
              scale-factor (Math/exp (* (- alpha) (- start-time prev-start-time)))]
          
          ;; rescale each key
          (doseq [k (keys samples)]
            (let [val (.remove samples k)]
              (.put samples (* scale-factor k) val)))
          
          ;; make sure counter reflects size of collection
          (.set counter (count samples)))))))

(deftype+ UniformSampler
  [^AtomicReferenceArray samples
   ^AtomicLong counter
   ^long sample-size]

  IUpdatable
  (update [_ val]
    (let [cnt (.incrementAndGet counter)]
      (if (<= cnt sample-size)

        ;; we don't have our full sample size, add everything
        (.set samples (dec cnt) val)

        ;; check to see if we should displace an existing sample
        (let [idx (long (* cnt (rand)))]
          (when (< idx sample-size)
            (.set samples idx val))))))

  clojure.lang.IDeref
  (deref [_]
    (let [cnt (min (.get counter) sample-size)
          ^objects ary (object-array cnt)]
      (dotimes [i cnt]
        (aset ary i (.get samples i)))
      (seq ary))))

;;;

(defn moving-sampler
  "Returns a sampler that is biased towards more recent results."
  [{:keys [sample-size window task-queue]
    :or {sample-size 1024
         window (t/hours 1)
         task-queue (t/task-queue)}}]
  (let [alpha (/ 8e-2 window) ;; magic numbers ahoy
        now (t/now task-queue)]
    (ExponentiallyDecayingSampler.
      (ConcurrentSkipListMap.)
      (AtomicLong. 0)
      (AtomicLong. (+ now rescale-interval))
      now
      (double alpha)
      (l/asymmetric-lock)
      task-queue
      sample-size)))

(defn sampler
  [{:keys [sample-size]
    :or {sample-size 1024}}]
  (UniformSampler.
    (AtomicReferenceArray. (long sample-size))
    (AtomicLong. 0)
    sample-size))
