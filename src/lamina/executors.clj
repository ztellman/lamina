;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executors
  (:use
    [lamina.core.pipeline]
    [lamina.core.channel :only (timed-channel receive)])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent
     TimeoutException
     ExecutorService
     ThreadPoolExecutor
     LinkedBlockingQueue
     TimeUnit
     ThreadFactory]))

;;;

(def *current-executor* nil)
(def default-executor (atom nil))
(def ns-executors (atom {}))

(defn set-default-executor
  "Sets the default executor used by task."
  [executor]
  (reset! default-executor executor))

(defn set-local-executor
  "Sets the executor used by task when called within the current namespace."
  [executor]
  (swap! ns-executors assoc *ns* executor))

(defmacro current-executor []
  `(or
     *current-executor*
     (@ns-executors ~*ns*)
     @default-executor
     clojure.lang.Agent/soloExecutor))

;;;

(defn thread-pool-statistics [^ThreadPoolExecutor pool]
  {:completed-tasks (.getCompletedTaskCount pool)
   :pending-tasks (- (.getTaskCount pool) (.getCompletedTaskCount pool))
   :active-threads (.getActiveCount pool)
   :thread-count (.getPoolSize pool)
   :thread-pool pool})

(defn log-thread-pool-statistics [name statistics]
  (log/info
    (str "Statistics for " name ":\n"
      "Thread Count:    " (:thread-count statistics) "   \n"
      "Active Threads:  " (:active-threads statistics) "   \n"
      "Pending Tasks:   " (:pending-tasks statistics) "   \n")))

(def default-options
  {:max-thread-count Integer/MAX_VALUE
   :min-thread-count 0
   :idle-threshold (* 60 1000)
   :thread-wrapper (fn [f] (.run ^Runnable f))
   :name "Generic Thread Pool"
   :stat-period -1
   :stat-callback log-thread-pool-statistics})

(defn interrupt-thread [^Thread thread]
  (.interrupt thread))

(defn thread-pool [options]
  (let [options (merge default-options options)
	pool (ThreadPoolExecutor.
	       (int (:min-thread-count options))
	       (int (:max-thread-count options))
	       (long (:idle-threshold options))
	       TimeUnit/MILLISECONDS
	       (LinkedBlockingQueue.)
	       (reify ThreadFactory
		 (newThread [_ f]
		   (Thread. #((:thread-wrapper options) f)))))]
    (when (pos? (:stat-period options))
      (run-pipeline nil
	(wait-stage (:stat-period options))
	(fn [_]
	  (when-not (.isTerminated pool)
	    ((:stat-callback options) (:name options) (thread-pool-statistics pool))
	    (restart)))))
    pool))

(defmacro with-thread-pool [pool & body]
  (let [options (when (map? (first body)) (first body))
	body (if options (rest body) body)]
    `(let [body-fn# (fn [] ~@body)
	   pool# ~pool]
       (if-not pool#
	 (body-fn#)
	 (let [result# (result-channel)
	       markers# (atom [(System/currentTimeMillis)])]
	   (.submit pool#
	     (fn []
	       (swap! markers# conj (System/currentTimeMillis))
	       (let [options# ~options]
		 (when (pos? (or (:timeout options#) 0))
		   (let [timeout# (:timeout options#)
			 thread# (Thread/currentThread)]
		     (run-pipeline nil
		       (wait-stage timeout#)
		       (fn [_#]
			 (receive (poll-result result# 0)
			   (fn [x#]
			     (when-not x#
			       (error! result#
				 (TimeoutException. (str "Timed out after " timeout# "ms")))
			       (interrupt-thread thread#)))))))))
	       (binding [*current-executor* pool#]
		 (siphon-result
		   (run-pipeline nil
		     :error-handler (constantly nil)
		     (fn [_#]
		       (let [inner-result# (body-fn#)]
			 (swap! markers# conj (System/currentTimeMillis))
			 inner-result#)))
		   result#))))
	   result#)))))
