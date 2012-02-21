(ns lamina.trace.timer
  (:use
    [useful.datatypes :only (make-record)]
    [lamina.core.utils :only (enqueue)])
  (:require
    [clojure.string :as str]
    [lamina.core.probe :as p]
    [lamina.core.result :as r]
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
      (enqueue (p/error-probe-channel [description :error]) {:error err}))
    (mark-enter [_])
    (mark-return [_ _])
    (add-sub-task [_ _] )))

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
    (set! enter (System/nanoTime)))
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (p/error-probe-channel [description :error])
      (assoc @this :error err)))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (when probe
      (enqueue probe
        (make-timing EnqueuedTiming
          :enqueued-duration (if (= Long/MIN_VALUE enter)
                               -1
                               (unchecked-subtract (long enter) (long enqueued)))
          :result val)))))

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
      (assoc @this :error err)))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (when probe
      (enqueue probe
        (make-timing Timing
          :result val)))))

(defn timer [description args probe-channel implicit?]
  (let [enabled? (and probe-channel
                   (p/probe-enabled? probe-channel))
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
        timer)
      (dummy-timer description))))

;;;

(defn indent [n s]
  (->> s
    str/split-lines
    (map #(str "  " %))
    (interpose "\n")
    (apply str)))

(def durations
  {"ns" 0
   "us" 1e3
   "ms" 1e6
   "s" 1e9})

(defn duration [n scale]
  (str
    (format "%.1f" (/ n (durations scale)))
    scale))

(defn format-duration [n]
  (cond
    (< n 1e3) (duration n "ns")
    (< n 1e6) (duration n "us")
    (< n 1e9) (duration n "ms")
    :else (duration n "s")))

(defn format-timing [t]
  (let [desc (:description t)
        desc (if (instance? clojure.lang.Named desc)
               (name desc)
               (str desc))
        duration (:duration t)
        enqueued (:enqueued-duration t)]
    (str/trim
      (str desc " - "
        (if duration
          (str
            (format-duration duration)
            (when enqueued
              (str " (" (format-duration enqueued) " enqueued)")))
          "still running")
        "\n"
        (indent 2
          (->> t
            :sub-tasks
            (map format-timing)
            (interpose "\n")
            (apply str)))))))


