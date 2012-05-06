(ns lamina.trace.timer
  (:use
    [potemkin]
    [useful.datatypes :only (make-record)]
    [lamina.core.utils :only (enqueue)])
  (:require
    [clojure.string :as str]
    [lamina.trace.probe :as p]
    [lamina.core.result :as r]
    [lamina.core.context :as context]
    [lamina.executor.utils :as ex])
  (:import
    [java.io
     Writer]
    [java.util.concurrent
     ConcurrentLinkedQueue]))

(set! *warn-on-reflection* true)

;;;

(defprotocol-once ITimed
  (timing [_ start])
  (mark-enter [_])
  (mark-error [_ err])
  (mark-return [_ val])
  (add-sub-task [_ timer]))

(defn dummy-timer [description enqueue-error?]
  (reify
    ITimed
    (timing [_ start]
      {})
    (mark-error [_ err]
      (when enqueue-error?
        (enqueue (p/error-probe-channel [description :error]) {:error err})))
    (mark-enter [_])
    (mark-return [_ _])
    (add-sub-task [_ _] )))

;;;

(defrecord EnqueuedTiming
  [description
   ^long offset
   ^long timestamp
   ^long duration
   ^long enqueued-duration
   sub-tasks
   args
   result])

(defmacro make-timing [type start & extras]
  `(let [start# ~start
         timing# (make-record ~type
                   :description ~'description
                   :timestamp ~'timestamp
                   :offset (unchecked-subtract (long ~'enter) (long start#))
                   :duration (if (or
                                   (= Long/MIN_VALUE ~'return)
                                   (= Long/MIN_VALUE ~'enter))
                               -1
                               (unchecked-subtract (long ~'return) (long ~'enter)))
                   :args ~'args
                   :result ~'result
                   :sub-tasks (when-not (.isEmpty ~'sub-tasks)
                                (doall (map #(timing % start#) ~'sub-tasks)))
                   ~@extras)]
     (if-not (neg? ~'start-stage)
       (assoc timing# :start-stage ~'start-stage)
       timing#)))

(deftype-once EnqueuedTimer
  [executor
   description
   return-probe
   error-probe
   ^long start-stage
   ^long timestamp
   ^long enqueued
   args
   ^{:volatile-mutable true} result
   ^{:volatile-mutable true, :tag long} enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-tasks]
  ITimed
  (timing [_ start]
    (make-timing EnqueuedTiming enqueued
      :enqueued-duration (if (= Long/MIN_VALUE enter)
                           -1
                           (unchecked-subtract (long enter) (long enqueued)))))
  (add-sub-task [_ timer]
    (.add sub-tasks timer))
  (mark-enter [this]
    (set! enter (System/nanoTime)))
  (mark-error [this err]
    (set! return (System/nanoTime))
    (let [timing (assoc (timing this enter) :error err)]
      (enqueue
        (or error-probe (p/error-probe-channel [description :error]))
        timing)
      (ex/trace-error executor timing)))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (set! result val)
    (let [timing (make-timing EnqueuedTiming enqueued
                   :enqueued-duration (if (= Long/MIN_VALUE enter)
                                        -1
                                        (unchecked-subtract (long enter) (long enqueued))))]
      (when return-probe
        (enqueue return-probe timing))
      (ex/trace-return executor timing))))

(defn enqueued-timer-
  [executor description args return-probe error-probe start-stage implicit? enqueue-error?]
  (let [enabled? (and return-probe (p/probe-enabled? return-probe))
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (EnqueuedTimer.
                    executor
                    description
                    (when enabled? return-probe)
                    error-probe
                    start-stage
                    (System/currentTimeMillis)
                    (System/nanoTime)
                    args
                    nil
                    Long/MIN_VALUE
                    Long/MIN_VALUE
                    (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-task parent timer))
        timer)
      (dummy-timer description enqueue-error?))))

(defmacro enqueued-timer
  [executor
   & {:keys [description
             args
             return-probe
             error-probe
             start-stage
             implicit?
             enqueue-error?]
      :or {implicit? false
           enqueue-error? true
           start-stage -1}}]
  `(enqueued-timer-
     ~executor
     ~description
     ~args
     ~return-probe
     ~error-probe
     ~start-stage
     ~implicit?
     ~enqueue-error?))

;;;

(defrecord Timing
  [description
   ^long timestamp
   ^long offset
   ^long duration
   sub-tasks
   args
   result])

(deftype-once Timer
  [description
   return-probe
   error-probe
   ^long start-stage
   args
   ^{:volatile-mutable true} result
   ^long timestamp
   ^long enter
   ^{:volatile-mutable true, :tag long} return
   ^ConcurrentLinkedQueue sub-tasks]
  ITimed
  (timing [_ start]
    (make-timing Timing start))
  (add-sub-task [_ timer]
    (.add sub-tasks timer))
  (mark-enter [_]
    )
  (mark-error [this err]
    (set! return (System/nanoTime))
    (enqueue
      (or error-probe (p/error-probe-channel [description :error]))
      (assoc (timing this enter) :error err)))
  (mark-return [this val]
    (set! return (System/nanoTime))
    (set! result val)
    (when return-probe
      (enqueue return-probe
        (make-timing Timing enter)))))

(defn timer-
  [description
   args
   return-probe
   error-probe
   start-stage
   implicit?
   enqueue-error?]
  (let [enabled? (and return-probe (p/probe-enabled? return-probe))
        parent (when (or enabled? implicit?)
                 (context/timer))]
    (if (or enabled? parent)
      (let [timer (Timer.
                    description
                    (when enabled? return-probe)
                    error-probe
                    start-stage
                    args
                    nil
                    (System/currentTimeMillis)
                    (System/nanoTime)
                    Long/MIN_VALUE
                    (ConcurrentLinkedQueue.))]
        (when parent
          (add-sub-task parent timer))
        timer)
      (dummy-timer description enqueue-error?))))

(defmacro timer
  [& {:keys [description
             args
             return-probe
             error-probe
             start-stage
             implicit?
             enqueue-error?]
      :or {implicit? false
           enqueue-error? true
           start-stage -1}}]
  `(timer-
     ~description
     ~args
     ~return-probe
     ~error-probe
     ~start-stage
     ~implicit?
     ~enqueue-error?))

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
        (cond
          (and (not duration) (not enqueued))
          "still running"

          (not enqueued)
          (format-duration duration)

          (not duration)
          (str "enqueued for " (format-duration enqueued) ", still running")

          :else
          (str (format-duration (+ enqueued duration))
            " (enqueued for " (format-duration enqueued) ", "
            "ran for " (format-duration duration) ")"))
        "\n"
        (indent 2
          (->> t
            :sub-tasks
            (map format-timing)
            (interpose "\n")
            (apply str)))))))


;;;

(defmethod print-method Timing [o ^Writer w]
  (.write w (pr-str (into {} o))))

(defmethod print-method EnqueuedTiming [o ^Writer w]
  (.write w (pr-str (into {} o))))
