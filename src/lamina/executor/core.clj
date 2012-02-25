;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor.core
  (:use
    [lamina.executor.utils]
    [lamina.core.threads :only (delay-invoke)])
  (:require
    [lamina.core.pipeline :as p]
    [lamina.core.result :as r]
    [lamina.trace.timer :as t]
    [lamina.core.context :as context])
  (:import
    [java.util.concurrent
     LinkedBlockingQueue
     ThreadPoolExecutor
     ThreadFactory
     TimeUnit]))

(set! *warn-on-reflection* true)

(defn contract-pool-size [^ThreadPoolExecutor pool]
  (let [active (.getActiveCount pool)
        pool-size (.getPoolSize pool)]
    (if (< active pool-size)
      (.setCorePoolSize pool (dec pool-size)))))

(defn expand-pool-size [^ThreadPoolExecutor pool max-thread-count]
  (let [active (.getActiveCount pool)
        pool-size (.getPoolSize pool)]
    (when (= pool-size active)
      (.setCorePoolSize pool (min max-thread-count (inc active))))))

(defn periodically-contract-pool-size [^ThreadPoolExecutor pool interval]
  (when-not (.isShutdown pool)
    (contract-pool-size pool)
    (delay-invoke interval #(periodically-contract-pool-size pool))))

(defn executor
  "Defines a thread pool that can be used with instrument and defn-instrumented.

   more goes here"
  [& {:keys [idle-timeout
             min-thread-count
             max-thread-count]
      :or {idle-timeout 60000
           min-thread-count 1
           max-thread-count Integer/MAX_VALUE}
      :as options}]
  (when-not (contains? options :name)
    (throw (IllegalArgumentException. "Every executor must have a :name specified.")))
  (let [nm (name (:name options))
        cnt (atom 0)
        pool (ThreadPoolExecutor.
                1
                1
                (long idle-timeout)
                TimeUnit/MILLISECONDS
                (LinkedBlockingQueue.)
                (reify ThreadFactory
                  (newThread [_ f]
                    (doto
                      (Thread. f)
                      (.setName (str nm "-" (swap! cnt inc)))))))]
    (periodically-contract-pool-size pool (* idle-timeout 1000))
    (reify
      clojure.lang.IDeref
      (deref [_]
        {:completed-tasks (.getCompletedTaskCount pool)
         :pending-tasks (- (.getTaskCount pool) (.getCompletedTaskCount pool))
         :active-threads (.getActiveCount pool)
         :num-threads (.getPoolSize pool)})
      IExecutor
      (shutdown [_]
        (.shutdown pool))
      (execute [this timer f timeout]
        (let [result (if timeout
                       (r/expiring-result timeout)
                       (r/result-channel))
              f (fn []
                  (when timeout
                    (let [thread (Thread/currentThread)]
                      (r/subscribe result
                        (r/result-callback
                          (fn [_])
                          (fn [ex]
                            (when (= :lamina/timeout! ex)
                              (.interrupt thread)))))))
                  (when timer (t/mark-enter timer))
                  (context/with-context (context/assoc-context :timer timer)
                    (p/run-pipeline nil
                      {:error-handler #(when timer (t/mark-error timer %))
                       :result result}
                      (fn [_]
                        (f))
                      (fn [x]
                        (when timer (t/mark-return timer x)) 
                        x))))]
          (expand-pool-size pool max-thread-count)
          (.execute pool f)
          result)))))

(def
  ^{:doc "A default executor with an unbounded maximum thread count."}
  default-executor (executor
                     :name "lamina-default-executor"
                     :idle-timeout 15000))
