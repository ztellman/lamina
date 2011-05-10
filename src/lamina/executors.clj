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
    [lamina logging])
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
   :hooks {:timeout default-timeout-handler}})

(defn thread-pool
  "Creates a thread pool that will grow to a specified size when necessary, and dispose
   of unused threads after a certain amount of inactivity.

   The following options may be specified:

   :min-thread-count - the lower boundary for the thread count (defaults to 0)
   :max-thread-count - the upper boundary for the thread count (defaults to Integer/MAX_VALUE)
   :idle-threshold - the interval before an inactive thread is reclaimed (defaults to 60,000ms)
   :name - the name used for logging
   :timeout - the maximum duration a thread may be in use by a single task

   under :hooks, the following log channels may be specified:

   :timeout   Called when a thread times out with:
                {:thread, :result, :timeout}
              By default, this will trigger a TimeoutException in the result and interrupt the
              thread.  

   :timing    Called every time a task completes with:
                {:enqueued-time, :start-time, :end-time,
                 :queue-duration, :execution-duration, :total-duration}
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
	   (doseq [ch (-> options :hooks vals)]
	     (close ch)))
	 (execute [_ f]
	   (when-let [state-hook (-> options :hooks :state)]
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
	      (when-let [timeout-hook (-> options :hooks :timeout)]
		(enqueue timeout-hook [thread result timeout])))))))))

(defn thread-timing [data options]
  (when-let [timing-hook (-> options :hooks :timing)]
    (let [[enqueued start end] data
	  enqueued (/ (double enqueued) 1e6)
	  start (/ (double start) 1e6)
	  end (/ (double end) 1e6)
	  queue-duration (- start enqueued)
	  execution-duration (- end start)]
      (enqueue timing-hook
	{:enqueued-time enqueued
	 :start-time start
	 :end-time end
	 :queue-duration queue-duration
	 :execution-duration execution-duration
	 :total-duration (+ queue-duration execution-duration)}))))

(defmacro with-thread-pool
  "Executes the body on the specified thread pool.  Returns a result-channel representing the
   eventual return value.

   The thread pool may be optionally followed by a map specifying options that override those
   given when the thread pool was created.  If, for instance, we want to have the timeout be
   100ms in this particular instance, we can use:


   (with-thread-pool pool {:timeout 100}
     ...)"
  [pool & body]
  (let [options? (> (count body) 1)]
    `(let [body-fn# (fn [] (run-pipeline ~@(if options? (rest body) body)))
	   pool# ~pool
	   options# (merge
		      (-> pool# meta ::options)
		      *thread-pool-options*
		      (when ~options?
			(let [first-arg# ~(first body)]
			  (when (map? first-arg#)
			    first-arg#))))]
       (if-not pool#
	 (body-fn#)
	 (let [result# (result-channel)
	       markers# (atom [(System/nanoTime)])]
	   (.execute ^Executor pool#
	     (fn []
	       (swap! markers# conj (System/nanoTime))
	       (lamina.executors/thread-timeout result# options#)
	       (binding [*current-executor* pool#
			 *thread-pool-options* options#]
		 (siphon-result
		   (run-pipeline nil
		     :error-handler (constantly nil)
		     (fn [_#]
		       (let [inner-result# (body-fn#)]
			 (lamina.executors/thread-timing
			   (conj @markers# (System/nanoTime)) options#)
			 inner-result#)))
		   result#))))
	   result#)))))

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
	  (let [options (merge options inner-options)
		timing-hook (-> options :hooks :timing)
		timing-channel (when timing-hook (channel))
		options (if timing-hook
			  (update-in options [:hooks :timing]
			    (fn [existing-hook]
			      (when existing-hook
				(siphon (fork timing-channel) {existing-hook identity}))
			      timing-channel))
			  options)]
	    (let [result (with-thread-pool pool options
			   (apply f args))]
	      (when timing-hook
		(run-pipeline result
		  (fn [val]
		    (close timing-channel)
		    (enqueue timing-hook {:args args, :timing (channel-seq timing-channel)}))))
	      result))))))
