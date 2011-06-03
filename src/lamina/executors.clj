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
(def default-executor (atom nil))
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
     *current-executor*
     (@ns-executors ~*ns*)
     @default-executor
     clojure.lang.Agent/soloExecutor))

(defprotocol LaminaThreadPool
  (shutdown-thread-pool [t]))

;;;

(defn- pending-tasks [^ThreadPoolExecutor pool]
  (- (.getTaskCount pool) (.getCompletedTaskCount pool)))

(defn- thread-pool-state [^ThreadPoolExecutor pool]
  {:completed-tasks (.getCompletedTaskCount pool)
   :pending-tasks (pending-tasks pool)
   :active-threads (.getActiveCount pool)
   :thread-count (.getPoolSize pool)})

(def ^{:private true} default-timeout-handler
  (let [ch (channel)]
    (receive-all ch
      (fn [info]
	(when-not (and (nil? info) (drained? ch))
	  (let [{thread :thread, result :result, timeout :timeout} info]
	    (error! result (TimeoutException. (str "Timed out after " timeout "ms.")))
	    (.interrupt ^Thread thread)))))
    ch))

(def ^{:private true} default-options
  {:max-thread-count Integer/MAX_VALUE
   :min-thread-count 0
   :idle-threshold (* 60 1000)
   :thread-wrapper (fn [f] (.run ^Runnable f))
   :name "Generic Thread Pool"
   :probes {:timeout default-timeout-handler}})

(defn thread-pool
  "Creates a thread pool that will grow to a specified size when necessary, and dispose
   of unused threads after a certain amount of inactivity.

   The following options may be specified:

   :min-thread-count - the lower boundary for the thread count (defaults to 0)
   :max-thread-count - the upper boundary for the thread count (defaults to Integer/MAX_VALUE)
   :idle-threshold - the interval before an inactive thread is reclaimed (defaults to 60,000ms)
   :name - the name used for logging
   :timeout - the maximum duration a thread may be in use by a single task

   under :probes, the following log channels may be specified:

   :timeout   Called when a thread times out with:
                {:thread, :result, :timeout}
              By default, this will trigger a TimeoutException in the result and interrupt the
              thread.  

   :timing    Called every time a task completes with:
                {:enqueued-time, :start-time, :end-time,
                 :queue-duration, :execution-duration, :duration}
              All values are in milliseconds.

   :state     Called every time a task is enqueued with:
                {:completed-tasks, :pending-tasks, :active-threads, :thread-count}
              "
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
	   (.shutdown pool)
	   (doseq [ch (-> options :probes vals)]
	     (close ch)))
	 (execute [_ f]
	   (when-let [state-hook (-> options :probes :state)]
	     (enqueue state-hook (thread-pool-state pool)))
	   (let [active (.getActiveCount pool)]
	     (if (= (.getPoolSize pool) active)
	       (.setCorePoolSize pool (min max-thread-count (inc active)))
	       (.setCorePoolSize pool (max min-thread-count (inc active)))))
	   (.execute pool f))))))

(defn thread-pool? [x]
  (instance? Executor x))

(defn thread-timeout [result options]
  (when-let [timeout (:timeout options)]
    (when-not (neg? timeout)
      (let [thread (Thread/currentThread)]
	(receive (poll-result result timeout)
	  (fn [x#]
	    (when-not x#
	      (when-let [timeout-hook (-> options :probes :timeout)]
		(enqueue timeout-hook [thread result timeout])))))))))

(defn thread-pool-timing [enqueued start end args result options]
  (let [timing (delay
		 (let [enqueued (/ (double enqueued) 1e6)
		       start (/ (double start) 1e6)
		       end (/ (double end) 1e6)
		       queue-duration (- start enqueued)
		       execution-duration (- end start)]
		   {:enqueued-time enqueued
		    :start-time start
		    :end-time end
		    :args args
		    :result result
		    :queue-duration queue-duration
		    :execution-duration execution-duration
		    :duration (+ queue-duration execution-duration)}))]
    (when-let [timing-hook (-> options :probes :timing)]
      (enqueue timing-hook @timing))
    (trace [(:name options) :timing]
      @timing)))

(defmacro with-thread-pool
  "Executes the body on the specified thread pool.  Returns a result-channel representing the
   eventual return value.

   The thread pool may be optionally followed by a map specifying options that override those
   given when the thread pool was created.  If, for instance, we want to have the timeout be
   100ms in this particular instance, we can use:


   (with-thread-pool pool {:timeout 100}
     ...)"
  [pool options & body]
  `(let [pool# ~pool
	 enqueued-time# (System/nanoTime)
	 options# (merge
		    (-> pool# meta ::options)
		    *thread-pool-options*
		    ~options)
	 body-fn# (fn []
		    (let [start-time# (System/nanoTime)
			  result# (run-pipeline (do ~@body))]
		      (run-pipeline result#
			(fn [r#]
			  (thread-pool-timing
			    enqueued-time#
			    start-time#
			    (System/nanoTime)
			    (:args options#)
			    r#
			    options#)))
		      result#))]
     (if-not pool#
       (body-fn#)
       (let [result# (result-channel)]
	 (.execute ^Executor pool#
	   (fn []
	     (lamina.executors/thread-timeout result# options#)
	     (binding [*current-executor* pool#
		       *thread-pool-options* options#]
	       (siphon-result
		 (run-pipeline nil
		   :error-handler (constantly nil)
		   (fn [_#]
		     (body-fn#)))
		 result#))))
	 result#))))

(defn executor
  "Given a thread pool and a function, returns a function that will execute that function
   on a thread pool and returns a result-channel representing its eventual value.

   The returned function takes a sequence of arguments as its first argument, and thread
   pool configurations as an optional second argument.

   > (def f +)
   #'f
   > (f 1 2)
   3
   > (def f* (executor (thread-pool) f))
   #'f*
   > @(f* [1 2] {:timeout 100})
   3"
  ([pool f]
     (executor pool f nil))
  ([pool f options]
     (fn this
       ([args]
	  (this args nil))
       ([args inner-options]
	  (let [options (merge options inner-options)]
	    (with-thread-pool pool (assoc options :args args)
	      (apply f args)))))))
