(ns lamina.trace.timer
  (:use
    [useful.datatypes :only (make-record)]
    [lamina.core.utils :only (enqueue)])
  (:require
    [lamina.core.probe :as p]
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
  (add-sub-task [_ timer]))

(defn dummy-timer [description]
  (reify
    clojure.lang.IDeref
    (deref [_] {})
    ITimed
    (mark-error [_ err]
      (enqueue (p/probe-channel [description :error]) {:error err}))
    (mark-enter [_])
    (mark-return [_ _])
    (add-sub-task [_ _] (assert false))))

;;;

(defrecord EnqueuedTiming
  [description
   ^long timestamp
   ^long duration
   ^long enqueued-duration
   sub-tasks
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
     :sub-tasks (when-not (.isEmpty ~'sub-tasks)
                  (doall (map deref ~'sub-tasks)))
     ~@extras))

(deftype EnqueuedTimer
  [description
   probe
   timestamp
   enqueued
   args
   ^{:volatile-mutable true, :tag long} enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-tasks]
  clojure.lang.IDeref
  (deref [_]
    (make-timing EnqueuedTiming
      :enqueued-duration (if (= Long/MIN_VALUE enter)
                           -1
                           (unchecked-subtract (long enter) (long enqueued)))))
  ITimed
  (add-sub-task [_ timer]
    (.add sub-tasks timer))
  (mark-enter [this]
    (set! enter (System/nanoTime))
    (context/push-context :timer this))
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (p/error-probe-channel [description :error])
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
  (let [enabled? (p/probe-enabled? probe-channel)
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (EnqueuedTimer.
                    description
                    (when enabled? probe-channel)
                    (System/currentTimeMillis)
                    (System/nanoTime)
                    args
                    Long/MIN_VALUE
                    Long/MIN_VALUE
                    (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-task parent timer))
        timer)
      (dummy-timer description))))

;;;

(defrecord Timing
  [description
   ^long timestamp
   ^long duration
   sub-tasks
   args
   result])

(deftype Timer
  [description
   probe
   args
   timestamp
   enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-tasks]
  clojure.lang.IDeref
  (deref [_]
    (make-timing Timing))
  ITimed
  (add-sub-task [_ timer]
    (.add sub-tasks timer))
  (mark-enter [_]
    )
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (p/error-probe-channel [description :error])
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
  (let [enabled? (p/probe-enabled? probe-channel)
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (Timer.
                    description
                    (when enabled? probe-channel)
                    args
                    (System/currentTimeMillis)
                    (System/nanoTime)
                    Long/MIN_VALUE
                    (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-task parent timer))
        (context/push-context :timer timer)
        timer)
      (dummy-timer description))))
