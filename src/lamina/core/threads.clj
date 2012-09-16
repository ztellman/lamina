;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.threads
  (:require
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     ExecutorService
     Executors
     ThreadFactory
     ScheduledThreadPoolExecutor
     TimeUnit]))

(set! *warn-on-reflection* true)

;;;

(defn num-cores []
  (.availableProcessors (Runtime/getRuntime)))

(defn ^ThreadFactory thread-factory [name-generator]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto
        (Thread. runnable)
        (.setName (name-generator))
        (.setDaemon true)))))

(let [cnt (atom 0)
      tf (thread-factory #(str "lamina-cleanup-" (swap! cnt inc)))]
  (def ^ScheduledThreadPoolExecutor cleanup-executor
    (ScheduledThreadPoolExecutor. (int (num-cores)) ^ThreadFactory tf)))

(defn enqueue-cleanup [f]
  (.execute cleanup-executor f))

(let [cnt (atom 0)
      tf (thread-factory #(str "lamina-scheduler-" (swap! cnt inc)))]
  (def ^ScheduledThreadPoolExecutor scheduled-executor
    (ScheduledThreadPoolExecutor. (int (num-cores)) ^ThreadFactory tf)))

(defn delay-invoke [interval ^Runnable f]
  (if-not (pos? interval)
    (f)
    (let [^Runnable f
          (fn []
            (try
              (f)
              (catch Throwable e
                (log/error e "Error in delayed invocation."))))]
      (.schedule scheduled-executor f (long (* 1e6 interval)) TimeUnit/NANOSECONDS)
      nil)))

;;;

