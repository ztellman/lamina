;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor.core
  (:use
    [lamina.executor.utils])
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
          (.execute pool f)
          (let [active (.getActiveCount pool)]
            (if (= (.getPoolSize pool) active)
              (.setCorePoolSize pool (min max-thread-count (inc active)))
              (.setCorePoolSize pool (max min-thread-count (inc active)))))
          result)))))

(def default-executor (executor :name "lamina-default-executor"))
