(ns lamina.trace.timer
  (:use
    [useful.datatypes :only (make-record)]
    [lamina.core :only (error-probe-channel probe-channel probe-enabled? enqueue)])
  (:require
    [lamina.core.context :as context])
  (:import
    [java.util.concurrent
     ConcurrentLinkedQueue]))

(set! *warn-on-reflection* true)

;;;

(defprotocol ITimed
  (mark-enter [_])
  (mark-error [_ err])
  (mark-return [_ val])
  (add-sub-timer [_ timer]))

(defn dummy-timer [description]
  (reify
    clojure.lang.IDeref
    (deref [_] {})
    ITimed
    (mark-error [_ err]
      (enqueue (probe-channel [description :error]) {:error err}))
    (mark-enter [_])
    (mark-return [_ _])
    (add-sub-timer [_ _] (assert false))))

;;;

(defrecord EnqueuedTiming
  [description
   ^long timestamp
   ^long duration
   ^long enqueued-duration
   sub-timings
   args
   result])

(defmacro make-timing [type & extras]
  `(make-record ~type
     :description ~'description
     :timestamp ~'timestamp
     :duration 0
     :duration (if (or
                     (= Long/MIN_VALUE ~'return)
                     (= Long/MIN_VALUE ~'enter))
                 -1
                 (unchecked-subtract (long ~'return) (long ~'enter)))
     :args ~'args
     :sub-timings (when-not (.isEmpty ~'sub-timers)
                    (map deref ~'sub-timers))
     ~@extras))

(deftype EnqueuedTimer
  [description
   probe
   timestamp
   enqueued
   args
   ^{:volatile-mutable true, :tag long} enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-timers]
  clojure.lang.IDeref
  (deref [_]
    (make-timing EnqueuedTiming
      :enqueued-duration (if (= Long/MIN_VALUE enter)
                           -1
                           (unchecked-subtract (long enter) (long enqueued)))))
  ITimed
  (add-sub-timer [_ timer]
    (.add sub-timers timer))
  (mark-enter [this]
    (set! enter (System/nanoTime))
    (context/push-context :timer this))
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (error-probe-channel [description :error])
      (assoc @this :error err))
    (context/pop-context))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (when probe
      (enqueue probe
        (make-timing EnqueuedTiming
          :enqueued-duration (if (= Long/MIN_VALUE enter)
                               -1
                               (unchecked-subtract (long enter) (long enqueued)))
          :result val)))
    (context/pop-context)))

(defn enqueued-timer [description args probe-channel implicit?]
  (let [enabled? (probe-enabled? probe-channel)
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (make-record EnqueuedTimer
                    :description description
                    :probe (when enabled? probe-channel)
                    :args args
                    :timestamp (System/currentTimeMillis)
                    :enqueued (System/nanoTime)
                    :enter Long/MIN_VALUE
                    :return Long/MIN_VALUE
                    :sub-timers (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-timer parent timer))
        timer)
      (dummy-timer description))))

;;;

(defrecord Timing
  [description
   ^long timestamp
   ^long duration
   sub-timings
   args
   result])

(deftype Timer
  [description
   probe
   args
   timestamp
   enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-timers]
  clojure.lang.IDeref
  (deref [_]
    (make-timing Timing))
  ITimed
  (add-sub-timer [_ timer]
    (.add sub-timers timer))
  (mark-enter [_]
    )
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (error-probe-channel [description :error])
      (assoc @this :error err))
    (context/pop-context))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (when probe
      (enqueue probe
        (make-timing Timing
          :result val)))
    (context/pop-context)))

(defn timer [description args probe-channel implicit?]
  (let [enabled? (probe-enabled? probe-channel)
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (make-record Timer
                    :description description
                    :probe (when enabled? probe-channel)
                    :args args
                    :timestamp (System/currentTimeMillis)
                    :enter (System/nanoTime)
                    :return Long/MIN_VALUE
                    :sub-timers (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-timer parent timer))
        (context/push-context :timer timer)
        timer)
      (dummy-timer description))))
