;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor.core
  (:use
    [lamina.core.utils]
    [lamina.executor.utils]
    [lamina.time :only (invoke-in invoke-repeatedly now)])
  (:require
    [lamina.trace.probe :as pr]
    [lamina.core.pipeline :as p]
    [lamina.core.result :as r]
    [lamina.trace.timer :as t]
    [lamina.core.context :as context])
  (:import
    [lamina.executor.utils
     IExecutor]
    [java.util.concurrent
     BlockingQueue
     SynchronousQueue
     LinkedBlockingQueue
     ThreadPoolExecutor
     ThreadFactory
     TimeUnit]))

(defn contract-pool-size [^ThreadPoolExecutor pool min-thread-count]
  (let [active (.getActiveCount pool)
        pool-size (.getPoolSize pool)]
    (if (< active pool-size)
      (let [delta (- pool-size active)]
        (.setCorePoolSize pool (max min-thread-count (int (- pool-size (/ delta 2)))))))))

(defn expand-pool-size [^ThreadPoolExecutor pool max-thread-count]
  (let [pending (- (.getTaskCount pool) (.getCompletedTaskCount pool))
        pool-size (.getPoolSize pool)]
    (when (<= pool-size pending)
      (.setCorePoolSize pool (min max-thread-count (int (* 1.5 pool-size)))))))

(defn periodically-adjust-pool-size [^ThreadPoolExecutor pool min-thread-count max-thread-count period]
  (invoke-repeatedly period
    (fn [cancel]
      (if (.isShutdown pool)
        (cancel)
        (do
          (contract-pool-size pool min-thread-count)
          (expand-pool-size pool max-thread-count))))))

(defn executor
  "Defines a thread pool that can be used with instrument and defn-instrumented.

   more goes here"
  [{:keys [idle-timeout
           adjust-interval
           min-thread-count
           max-thread-count
           interrupt?]
    :or {idle-timeout 60000
         adjust-interval 1000
         interrupt? false}
    :as options}]
  (when-not (contains? options :name)
    (throw (IllegalArgumentException. "Every executor must have a :name specified.")))
  (let [nm (name (:name options))
        return-probe (pr/probe-channel [nm :return])
        error-probe (pr/probe-channel [nm :error])

        bounded? (boolean max-thread-count)
        min-thread-count (int (or min-thread-count 1))
        max-thread-count (int (if bounded? min-thread-count Integer/MAX_VALUE))

        ^BlockingQueue q (if bounded?
                           (LinkedBlockingQueue.)
                           (SynchronousQueue.))

        cnt (atom 0)
        ^ThreadFactory tf (reify ThreadFactory
                            (newThread [_ f]
                              (doto
                                (Thread. f)
                                (.setName (str nm "-" (swap! cnt inc))))))
        
        pool (ThreadPoolExecutor.
               min-thread-count
               max-thread-count
               (long idle-timeout)
               TimeUnit/MILLISECONDS
               q
               tf)
        
        stats (fn []
                {:name nm
                 :completed-tasks (.getCompletedTaskCount pool)
                 :pending-tasks (- (.getTaskCount pool) (.getCompletedTaskCount pool))
                 :active-threads (.getActiveCount pool)
                 :num-threads (.getPoolSize pool)})
        stats-channel (pr/probe-channel [nm :stats])]

    (when bounded?
      (periodically-adjust-pool-size pool min-thread-count max-thread-count adjust-interval))
    
    (invoke-repeatedly 1000
      (fn [cancel]
        (if (.isShutdown pool)
          (cancel)
          (when (pr/probe-enabled? stats-channel)
            (enqueue stats-channel (stats))))))

    (reify

      clojure.lang.IDeref
      (deref [_]
        (stats))

      IExecutor
      (probe-enabled? [_]
        (or
          (pr/probe-enabled? return-probe)
          (pr/probe-enabled? error-probe)))
      (trace-return [_ val]
        (when (pr/probe-enabled? return-probe)
          (enqueue return-probe val)))
      (trace-error [_ val]
        (when (pr/probe-enabled? error-probe)
          (enqueue error-probe val)))
      (shutdown [_]
        (.shutdown pool))
      (execute [this timer f timeout]
        (let [result (if timeout
                       (r/expiring-result timeout)
                       (r/result-channel))
              
              complete? (when interrupt? (atom false))

              f (fn []

                  ;; set up the thread interruption
                  (when (and interrupt? timeout)
                    (let [thread (Thread/currentThread)]
                      (r/subscribe result
                        (r/result-callback
                          (fn [_])
                          (fn [ex]
                            (when (and (identical? :lamina/timeout! ex)
                                    (compare-and-set! complete? false true))
                              (.interrupt thread)))))))

                  ;; mark the entry
                  (when timer (t/mark-enter timer))

                  ;; run the task
                  (p/run-pipeline nil
                    {:error-handler #(when timer (t/mark-error timer %))
                     :result result}
                    (fn [_]
                      (let [result (context/with-context (context/assoc-context :timer timer)
                                     (f))]
                        (when (r/async-promise? result)
                          (when timer
                            (t/mark-waiting timer)))
                        result))
                    (fn [result]
                      (when timer (t/mark-return timer result)) 
                      result))

                  ;; mark completion so we don't try to interrupt another task,
                  ;; and reset interrupt status
                  (when interrupt?
                    (reset! complete? true)
                    (Thread/interrupted)))]
          
          (.execute pool f)

          result)))))

(def
  ^{:doc "A default executor with an unbounded maximum thread count."}
  default-executor (executor
                     {:name "lamina-default-executor"
                      :idle-timeout 15000}))

(defmacro defexecutor [name options]
  (let [default-name (str (-> (ns-name *ns*) str (.replace \. \:)) ":" (str name))]
    `(def ~name
       (executor
         (merge
           {:name ~default-name}
           ~options)))))
