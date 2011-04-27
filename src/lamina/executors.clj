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
    [lamina.core.channel :only (close channel enqueue receive)]
    [lamina.core.seq :only (receive-all)]
    [lamina logging])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent
     ExecutorService
     Executor
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

(defprotocol LaminaExecutor
  (shutdown-executor [t]))

;;;

(defn pending-tasks [^ThreadPoolExecutor pool]
  (- (.getTaskCount pool) (.getCompletedTaskCount pool)))

(defn thread-pool-state [^ThreadPoolExecutor pool]
  {:completed-tasks (.getCompletedTaskCount pool)
   :pending-tasks (pending-tasks pool)
   :active-threads (.getActiveCount pool)
   :thread-count (.getPoolSize pool)})

(def default-options
  {:max-thread-count Integer/MAX_VALUE
   :min-thread-count 1
   :idle-threshold (* 60 1000)
   :thread-wrapper (fn [f] (.run ^Runnable f))
   :name "Generic Thread Pool"
   :hooks {:timeout default-timeout-handler}})

(defn thread-pool [options]
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
    (reify Executor LaminaExecutor
      (shutdown-executor [_]
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
	(.execute pool f)))))

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
	  queue-duration (-> start (- enqueued) (/ 1e6))
	  execution-duration (-> end (- start) (/ 1e6))]
      (enqueue timing-hook
	{:enqueued-time enqueued
	 :start-time start
	 :end-time end
	 :queue-duration queue-duration
	 :execution-duration execution-duration
	 :total-duration (+ queue-duration execution-duration)}))))

(defmacro with-thread-pool [pool & body]
  (let [options (when (map? (first body)) (first body))
	body (if options (rest body) body)]
    `(let [body-fn# (fn [] ~@body)
	   pool# ~pool
	   options# (merge (-> ~pool meta ::options) *thread-pool-options* ~options)]
       (if-not pool#
	 (body-fn#)
	 (let [result# (result-channel)
	       markers# (atom [(System/nanoTime)])]
	   (.execute ^Executor pool#
	     (fn []
	       (lamina.executors/thread-timeout result# options#)
	       (swap! markers# conj (System/nanoTime))
	       (binding [*current-executor* pool#]
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
