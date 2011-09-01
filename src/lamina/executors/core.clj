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
     lamina.core.pipeline/*current-executor*
     (@lamina.executors.core/ns-executors ~*ns*)
     @lamina.executors.core/default-executor))

(defprotocol LaminaThreadPool
  (shutdown-thread-pool [t])
  (timeouts-probe [t])
  (threads-probe [t])
  (results-probe [t])
  (errors-probe [t]))

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

(defn default-options []
  {:max-thread-count Integer/MAX_VALUE
   :min-thread-count 0
   :idle-threshold (* 60 1000)
   :thread-wrapper (fn [f] (.run ^Runnable f))
   :name (str (gensym "thread-pool."))})

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
     (let [options (merge-with #(if (map? %1) (merge %1 %2) %2) (default-options) options)
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
		      (Thread. #((:thread-wrapper options) f)))))
	   threads-probe (canonical-probe [(:name options) :threads])
	   results-probe (canonical-probe [(:name options) :results])
	   errors-probe (canonical-probe [(:name options) :errors])
	   timeouts-probe (canonical-probe [(:name options) :timeouts])]
       (register-probe threads-probe results-probe errors-probe)
       (siphon-probes (:name options) (:probes options))
       ^{::options options}
       (reify Executor LaminaThreadPool
	 (shutdown-thread-pool [_]
	   (.shutdown pool))
	 (threads-probe [_] threads-probe)
	 (results-probe [_] results-probe)
	 (errors-probe [_] errors-probe)
	 (timeouts-probe [_] timeouts-probe)
	 (execute [this f]
	   (trace threads-probe
	     (thread-pool-state pool))
	   (let [active (.getActiveCount pool)]
	     (if (= (.getPoolSize pool) active)
	       (.setCorePoolSize pool (min max-thread-count (inc active)))
	       (.setCorePoolSize pool (max min-thread-count (inc active)))))
	   (.execute pool
	     (fn []
	       (binding [*current-executor* this]
		 (f)))))))))

(def default-executor (atom (thread-pool {:name "thread-pool:default"})))

(defn thread-pool? [x]
  (instance? Executor x))

(defn thread-timeout [start args result probe options]
  (when-let [timeout (:timeout options)]
    (when-not (neg? timeout)
      (let [thread (Thread/currentThread)
	    begin (System/nanoTime)
	    remaining-timeout (max 0 (- timeout (int (/ (- begin start) 1e6))))]
	(receive (poll-result result remaining-timeout)
	  (fn [x#]
	    (when-not x#
	      (let [start (/ (double start) 1e6)
		    end (/ (double (System/nanoTime)) 1e6)]
		(when-let [timeout-handler (:timeout-handler options)]
		  (timeout-handler {:result-channel result, :thread thread, :timeout timeout}))
		(trace probe
		  {:start-time start
		   :end-time end
		   :args args
		   :timeout timeout
		   :duration (- end start)})
		(error! result (TimeoutException. (str "Timed out after " (int (- end start)) "ms")))
		(when-let [timeout-callback (:timeout-callback options)]
		  (timeout-callback thread))
		(.interrupt ^Thread thread)))))))))

(defn thread-pool-result
  [enqueued start args result probe options]
  (trace probe
    (let [result-transform (if-let [result-transform (:result-transform options)]
			     result-transform
			     identity)
	  args-transform (if-let [transform-fn (:args-transform options)]
			   transform-fn
			   identity)
	  enqueued (/ (double enqueued) 1e6)
	  start (/ (double start) 1e6)
	  end (/ (double (System/nanoTime)) 1e6)
	  queue-duration (- start enqueued)
	  execution-duration (- end start)]
      {:enqueued-time enqueued
       :start-time start
       :end-time end
       :args (args-transform args)
       :result (result-transform result)
       :queue-duration queue-duration
       :execution-duration execution-duration
       :duration (+ queue-duration execution-duration)})))

(defn thread-pool-error
  [enqueued start args ex probe options]
  (trace probe
    (let [args-transform (if-let [transform-fn (:args-transform options)]
			   transform-fn
			   identity)
	  enqueued (/ (double enqueued) 1e6)
	  start (/ (double start) 1e6)
	  end (/ (double (System/nanoTime)) 1e6)
	  queue-duration (- start enqueued)
	  execution-duration (- end start)]
      {:enqueued-time enqueued
       :start-time start
       :end-time end
       :args (args-transform args)
       :exception ex
       :queue-duration queue-duration
       :execution-duration execution-duration
       :duration (+ queue-duration execution-duration)})))

(defn expand-with-thread-pool
  [pool options & body]
  `(let [pool# ~pool
	 enqueued-time# (System/nanoTime)
	 options# (merge
		    (-> pool# meta ::options)
		    *thread-pool-options*
		    ~options)
	 body-fn# (fn []
		    (let [start-time# (System/nanoTime)
			  result# (run-pipeline nil
				    :error-handler (fn [_#])
				    (fn [_#]
				      (try
					~@body
					(catch InterruptedException e#
					  (throw (TimeoutException. (str "Timed out after " (-> (- (System/nanoTime) enqueued-time#) (/ 1e6) int) "ms")))))))]
		      (run-pipeline result#
			:error-handler (fn [ex#]
					 (when pool#
					   (thread-pool-error
					     enqueued-time#
					     start-time#
					     (:args options#)
					     ex#
					     (errors-probe pool#)
					     options#)))
			(fn [r#]
			  (when pool#
			    (thread-pool-result
			      enqueued-time#
			      start-time#
			      (:args options#)
			      r#
			      (results-probe pool#)
			      options#))))
		      result#))]
     (if-not pool#
       (run-pipeline nil
	 :error-handler (fn [_#])
	 (fn [_#] (body-fn#)))
       (let [result# (result-channel)]
	 (.execute ^Executor pool#
	   (fn []
	     (lamina.executors.core/thread-timeout
	       enqueued-time#
	       (:args options#)
	       result#
	       (timeouts-probe pool#)
	       options#)
	     (binding [*thread-pool-options* options#]
	       (siphon-result
		 (run-pipeline nil
		   :error-handler (fn [_#])
		   (fn [_#]
		     (body-fn#)))
		 result#))))
	 result#))))
