;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor.core
  (:use
    [lamina.core])
  (:require
    [lamina.trace.timer :as t])
  (:import
    [java.util.concurrent
     LinkedBlockingQueue
     ThreadPoolExecutor
     ThreadFactory
     TimeUnit]))

(set! *warn-on-reflection* true)

(defprotocol IExecutor
  (execute [_ timer f] [_ timer f timeout])
  (shutdown [_] "Shuts down the thread pool, making it impossible for any further tasks to be enqueued."))

(defn executor [& {:keys [idle-timeout
                          min-thread-count
                          max-thread-count]
                   :or {idle-timeout 60
                        min-thread-count 0
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
                TimeUnit/SECONDS
                (LinkedBlockingQueue.)
                (reify ThreadFactory
                  (newThread [_ f]
                    (doto
                      (Thread. f)
                      (.setName (str nm "-" (swap! cnt inc)))))))]
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
      (execute [this timer f]
        (let [result (result-channel)
              f (fn []
                  (let [active (.getActiveCount pool)]
                    (if (= (.getPoolSize pool) active)
                      (.setCorePoolSize pool (min max-thread-count (inc active)))
                      (.setCorePoolSize pool (max min-thread-count (inc active)))))
                  (t/mark-enter timer)
                   (run-pipeline nil
                     {:error-handler #(t/mark-error timer %)
                      :result result}
                     (fn [_]
                       (f))
                     (fn [x]
                       (t/mark-return timer x) 
                       x)))]
          (.execute pool f)
          result)))))
