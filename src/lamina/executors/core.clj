;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executors.core
  (:use
    [lamina.core.pipeline]
    [lamina.core.channel :only (close channel enqueue receive drained?)]
    [lamina.core.seq :only (receive-all siphon fork channel-seq)]
    [lamina trace])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent
     ExecutorService
     Executor
     TimeoutException
     ThreadPoolExecutor
     LinkedBlockingQueue
     TimeUnit
     ThreadFactory]))

;;;

(def *current-executor* nil)
(declare default-executor)
(def ns-executors (atom {}))

(def *thread-pool-options* nil)

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
     lamina.executors.core/*current-executor*
     (@lamina.executors.core/ns-executors ~*ns*)
     @lamina.executors.core/default-executor))

(defprotocol LaminaThreadPool
  (shutdown-thread-pool [t]))

;;;

(defn pending-tasks [^ThreadPoolExecutor pool]
  (- (.getTaskCount pool) (.getCompletedTaskCount pool)))

(defn thread-pool-state [^ThreadPoolExecutor pool]
  {:completed-tasks (.getCompletedTaskCount pool)
   :pending-tasks (pending-tasks pool)
   :active-threads (.getActiveCount pool)
   :thread-count (.getPoolSize pool)})

(defn default-timeout-handler [{:keys [thread result-channel timeout]}]
  (error! result-channel (TimeoutException. (str "Timed out after " timeout "ms.")))
  (.interrupt ^Thread thread))

(def default-options
  {:max-thread-count Integer/MAX_VALUE
   :min-thread-count 0
   :idle-threshold (* 60 1000)
   :thread-wrapper (fn [f] (.run ^Runnable f))
   :name "Generic Thread Pool"})

(defn thread-pool
  "Creates a thread pool that will grow to a specified size when necessary, and dispose
   of unused threads after a certain amount of inactivity.

   The following options may be specified:

   :min-thread-count - the lower boundary for the thread count (defaults to 0)
   :max-thread-count - the upper boundary for the thread count (defaults to Integer/MAX_VALUE)
   :idle-threshold - the interval before an inactive thread is reclaimed (defaults to 60,000ms)
   :name - the name used for logging
   :timeout - the maximum duration a thread may be in use by a single task."
  ([]
     (thread-pool {}))
  ([options]
     (let [options (merge-with #(if (map? %1) (merge %1 %2) %2) default-options options)
	   max-thread-count (:max-thread-count options)
	   min-thread-count (:min-thread-count options)
	   pool (ThreadPoolExecutor.
		  1
		  1
		  (long (:idle-threshold options))
		  TimeUnit/MILLISECONDS
		  (LinkedBlockingQueue.)
		  (reify ThreadFactory
		    (newThread [_ f]
		      (Thread. #((:thread-wrapper options) f)))))]
       ^{::options options}
       (reify Executor LaminaThreadPool
	 (shutdown-thread-pool [_]
	   (.shutdown pool))
	 (execute [_ f]
	   (when-let [state-hook (-> options :probes :state)]
	     (enqueue state-hook (thread-pool-state pool)))
	   (let [active (.getActiveCount pool)]
	     (if (= (.getPoolSize pool) active)
	       (.setCorePoolSize pool (min max-thread-count (inc active)))
	       (.setCorePoolSize pool (max min-thread-count (inc active)))))
	   (.execute pool f))))))

(def default-executor (atom (thread-pool {:name "thread-pool.default"})))

(defn thread-pool? [x]
  (instance? Executor x))

(defn thread-timeout [result options]
  (when-let [timeout (:timeout options)]
    (when-not (neg? timeout)
      (let [thread (Thread/currentThread)]
	(receive (poll-result result timeout)
	  (fn [x#]
	    (when-not x#
	      (when-let [timeout-handler (:timeout-handler options)]
		(timeout-handler {:result-channel result, :thread thread, :timeout timeout}))
	      (error! result (TimeoutException. (str "Timed out after " timeout "ms.")))
	      (.interrupt ^Thread thread))))))))

(defn thread-pool-call [enqueued start args result options]
  (trace [(:name options) :calls]
    (let [enqueued (/ (double enqueued) 1e6)
	  start (/ (double start) 1e6)
	  end (/ (double (System/nanoTime)) 1e6)
	  queue-duration (- start enqueued)
	  execution-duration (- end start)]
      {:enqueued-time enqueued
       :start-time start
       :end-time end
       :args args
       :result result
       :queue-duration queue-duration
       :execution-duration execution-duration
       :duration (+ queue-duration execution-duration)})))

(defn thread-pool-error [enqueued start args ex options]
  (trace [(:name options) :errors]
    (let [enqueued (/ (double enqueued) 1e6)
	  start (/ (double start) 1e6)
	  end (/ (double (System/nanoTime)) 1e6)
	  queue-duration (- start enqueued)
	  execution-duration (- end start)]
      {:enqueued-time enqueued
       :start-time start
       :end-time end
       :args args
       :exception ex
       :queue-duration queue-duration
       :execution-duration execution-duration
       :duration (+ queue-duration execution-duration)})))

(defn expand-with-thread-pool
  [pool options & body]
  `(let [pool# ~pool
	 enqueued-time# (System/nanoTime)
	 options# (merge
		    {:name (gensym "thread-pool.")}
		    (-> pool# meta ::options)
		    *thread-pool-options*
		    ~options)
	 body-fn# (fn []
		    (let [start-time# (System/nanoTime)
			  result# (run-pipeline (do ~@body))]
		      (run-pipeline result#
			:error-handler (fn [ex#]
					 (thread-pool-error
					   enqueued-time#
					   start-time#
					   (:args options#)
					   ex#
					   options#))
			(fn [r#]
			  (thread-pool-call
			    enqueued-time#
			    start-time#
			    (:args options#)
			    r#
			    options#)))
		      result#))]
     (if-not pool#
       (body-fn#)
       (let [result# (result-channel)]
	 (.execute ^Executor pool#
	   (fn []
	     (lamina.executors.core/thread-timeout result# options#)
	     (binding [*current-executor* pool#
		       *thread-pool-options* options#]
	       (siphon-result
		 (run-pipeline nil
		   :error-handler (constantly nil)
		   (fn [_#]
		     (body-fn#)))
		 result#))))
	 result#))))
