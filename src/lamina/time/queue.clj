;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.time.queue
  (:use
    [lamina.core.utils]
    [potemkin])
  (:require
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     ThreadFactory
     TimeUnit
     ThreadPoolExecutor
     ScheduledThreadPoolExecutor
     LinkedBlockingQueue
     ConcurrentSkipListSet]))

(defprotocol+ IClock
  (now [_]))

(defprotocol+ ITaskQueue
  (invoke-in- [_ delay f]))

;;;

(let [queue-factory (thread-factory (constantly "lamina-scheduler-queue"))
      task-queue    (ScheduledThreadPoolExecutor. 1 ^ThreadFactory queue-factory)

      cnt (atom 0)
      task-factory (thread-factory #(str "lamina-scheduler-" (swap! cnt inc)))
      task-executor (ThreadPoolExecutor.
                      (int (num-cores))
                      Integer/MAX_VALUE
                      (long 60)
                      TimeUnit/SECONDS
                      (LinkedBlockingQueue.)
                      ^ThreadFactory task-factory)]

  (def default-task-queue
    (reify
      ITaskQueue
      (invoke-in- [_ delay f]
        (let [enqueue-fn (fn []
                           (.execute task-executor
                             #(try
                                (f)
                                (catch Throwable e
                                  (log/error e "Error in delayed invocation")))))]
          (if (<= delay 0)
            (enqueue-fn)
            (.schedule task-queue
              ^Runnable enqueue-fn
              (long (* 1e6 delay))
              TimeUnit/NANOSECONDS)))
        true)
      IClock
      (now [_]
        (System/currentTimeMillis)))))

(def ^{:dynamic true} *task-queue* default-task-queue)

(defn task-queue
  "Returns the current task queue. Defaults to a real-time task queue."
  []
  *task-queue*)

(defmacro with-task-queue
  "Executes the body within a context where 'q' is the task-queue."
  [q & body]
  `(binding [lamina.time.queue/*task-queue* ~q]
     ~@body))

(defn invoke-in
  "Delays invocation of a function by 'delay' milliseconds."
  ([delay f]
     (invoke-in (task-queue) delay f))
  ([task-queue delay f]
     (invoke-in- task-queue delay f)))

(defn invoke-at
  "Delays invocation of a function until 'timestamp'."
  ([timestamp f]
     (invoke-at (task-queue) timestamp f))
  ([task-queue timestamp f]
     (invoke-in- task-queue (- timestamp (now task-queue)) f)))

(defn invoke-repeatedly
  "Repeatedly invokes a function every 'period' milliseconds, but ensures that the function cannot
   overlap its own invocation if it takes more than the period to complete.

   The function will be given a single parameter, which is a callback that can be invoked to cancel
   future invocations."
  ([period f]
     (invoke-repeatedly (task-queue) period f))
  ([task-queue period f]
     (let [target-time (atom (+ (now task-queue) period))
           latch (atom false)
           cancel-callback #(reset! latch true)
           
           schedule-next (fn schedule-next []
                           (invoke-in- task-queue (max 0.1 (- @target-time (now task-queue)))
                             (fn []
                               (try
                                 (f cancel-callback)
                                 (finally
                                   (when-not @latch
                                     (swap! target-time + period)
                                     (schedule-next)))))))]
       (schedule-next)
       true)))

;;;

(defprotocol+ INonRealTimeTaskQueue
  (advance [_] "Advances to the next task. Returns false if there are no remaining tasks.")
  (advance-until [_ timestamp] "Advances across all tasks that occur before or on the given timestamp."))

(defrecord+ TaskTuple [^long timestamp f]
  Comparable
  (compareTo [this o]
    (let [^TaskTuple o o
          c (compare timestamp (.timestamp o))]
      (if (zero? c)
        (compare (hash f) (hash (.f o)))
        c))))

(defn non-realtime-task-queue
  "A task queue which can be used to schedule timed or periodic tasks at something
   other than realtime."
  ([]
     (non-realtime-task-queue 0 false))
  ([start-time discard-past-events?]
     (let [tasks (ConcurrentSkipListSet.)
           now (atom start-time)]
       (reify
         ITaskQueue
         (invoke-in- [_ delay f]
           (if (and discard-past-events? (neg? delay))
             false
             (do
               (.add tasks (TaskTuple. (+ @now delay) f))
               true)))
         IClock
         (now [_]
           @now)
         INonRealTimeTaskQueue
         (advance [_]
           (when-let [^TaskTuple task (.pollFirst tasks)]
             (let [timestamp (.timestamp task)]
               (reset! now timestamp)
               ((.f task))
               timestamp)))

         (advance-until [this timestamp]
           (loop []
             (when-not (.isEmpty tasks)
               (let [^TaskTuple task (.first tasks)]
                 (when (<= (.timestamp task) timestamp)
                   (advance this)
                   (recur))))))))))

