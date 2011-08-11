;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.pipeline
  (:use
    [clojure.contrib.def :only (defmacro- defvar)]
    [lamina.core.channel]
    [clojure.pprint])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent
     TimeoutException
     Executor]))

;;;

(def *inside-pipeline?* false)

(def *current-executor* nil)

(defmacro with-executor [executor & body]
  `(let [f# (fn [] ~@body)]
     (if-let [executor# ~executor]
       (.execute ^Executor executor# f#)
       (f#))))

(defmacro with-executor* [executor & body]
  `(let [f# (fn [] ~@body)]
     (if-let [executor# ~executor]
       (if-not (= executor# *current-executor*)
	 (.execute ^Executor executor# f#)
	 (f#))
       (f#))))

;;;

(declare wait-for-result)

(deftype ResultChannel [success error metadata]
  Object
  (toString [_]
    (cond
      (not= ::none (dequeue success ::none))
      (str "<< " (pr-str (dequeue success nil)) " >>")

      (not= ::none (dequeue error ::none))
      (str "<< ERROR: " (dequeue error nil) " >>")

      :else
      "<< ... >>"))
  clojure.lang.IDeref
  (deref [this] (wait-for-result this))

  clojure.lang.IObj
  (withMeta [_ meta] (ResultChannel. success error meta))

  clojure.lang.IMeta
  (meta [_] metadata))

(defn ^ResultChannel result-channel []
  (ResultChannel. (constant-channel) (constant-channel) nil))

(defn ^ResultChannel error-result [val]
  (ResultChannel. nil-channel (constant-channel val) nil))

(defn ^ResultChannel success-result [val]
  (ResultChannel. (constant-channel val) nil-channel nil))

(defn result-channel? [x]
  (instance? ResultChannel x))

(defn on-success [^ResultChannel ch & callbacks]
  (apply receive (.success ch) callbacks))

(defn on-error [^ResultChannel ch & callbacks]
  (apply receive (.error ch) callbacks))

(defn success! [^ResultChannel ch value]
  (enqueue (.success ch) value))

(defn error! [^ResultChannel ch value]
  (enqueue (.error ch) value))

(defn poll-result
  ([result-channel]
     (poll-result result-channel -1))
  ([^ResultChannel result-channel timeout]
     (poll {:success (.success result-channel) :error (.error result-channel)} timeout)))

;;;

(defrecord Redirect [pipeline value])

(defn redirect
  "Returns a redirect signal, which if returned by a pipeline stage will
   skip all remaining stages in the current pipeline, and begin executing
   the stages in 'pipeline'.  'value' describes the initial value passed into
   the new pipeline, and defaults to the initial value passed into the current
   pipeline."
  ([pipeline]
     (Redirect. pipeline ::initial))
  ([pipeline value]
     (Redirect. pipeline value)))

(defn redirect? [x]
  (instance? Redirect x))

(defn restart
  "A special form of redirect, which simply restarts the current pipeline.  'value'
   describe sthe initial value passed into the first stage of the current pipeline,
   and defaults to the value that was previously passed into the first stage."
  ([]
     (restart ::initial))
  ([value]
     (redirect ::pipeline value)))

;;;

(defn handle-error [pipeline ^ResultChannel result ^ResultChannel outer-result]
  (let [ex (dequeue (.error result) nil)]
    (if-let [redirect (if-let [handler (:error-handler pipeline)]
			(let [result (handler ex)]
			  (when (redirect? result)
			    result)))]
      redirect
      (do
	(enqueue (.error outer-result) ex)
	nil))))

(defn process-redirect [redirect pipeline initial-value]
  (let [pipeline* (-> redirect :pipeline)
	pipeline* (if (= ::pipeline pipeline*)
		    pipeline
		    (-> pipeline* meta :pipeline))
	value (:value redirect)
	value (if (= ::initial value)
		initial-value
		value)]
    [pipeline* value]))

(defmacro redirect-recur [redirect pipeline initial-value err-count]
  `(let [[pipeline# value#] (process-redirect ~redirect ~pipeline ~initial-value)]
     (recur (:stages pipeline#) pipeline# value# value# ~err-count)))

(defn start-pipeline
  ([pipeline initial-value]
     (start-pipeline pipeline initial-value (result-channel)))
  ([pipeline initial-value result]
     (start-pipeline pipeline (:stages pipeline) initial-value initial-value result))
  ([pipeline fns value initial-value ^ResultChannel result]
     (binding [*inside-pipeline?* true]
       (loop [fns fns, pipeline pipeline, initial-value initial-value, value value, err-count 0]
	 (cond
	   (< 100 err-count)
	   (error! result (Exception. "Error loop detected in pipeline."))
	   
	   (redirect? value)
	   (redirect-recur value pipeline initial-value err-count)
	   
	   (result-channel? value)
	   (let [ch ^ResultChannel value]
	     (cond
	       (not= ::none (dequeue (.error ch) ::none))
	       (if-let [redirect (handle-error pipeline ch result)]
		 (redirect-recur redirect pipeline initial-value (inc err-count)))
	       
	       (not= ::none (dequeue (.success ch) ::none))
	       (recur fns pipeline initial-value (dequeue (.success ch) nil) 0)
	       
	       :else
	       (let [bindings (get-thread-bindings)]
		 (receive (poll-result value)
		   (fn [[outcome value]]
		     (with-executor (:executor pipeline)
		       (with-bindings bindings
			 (case outcome
			   :error (when-let [redirect (handle-error pipeline ch result)]
				    (let [[pipeline value] (process-redirect
							     redirect
							     pipeline
							     initial-value)]
				      (start-pipeline pipeline value result)))
			   :success (start-pipeline
				      pipeline fns
				      value initial-value
				      result)))))))))
	   
	   (empty? fns)
	   (success! result value)
	   
	   :else
	   (let [f (first fns)]
	     (let [[success val] (try
				   [true (f value)]
				   (catch Exception e
				     [false e]))]
	       (if success
		 (recur (rest fns) pipeline initial-value val 0)
		 (if-let [redirect (handle-error
				     pipeline
				     (error-result val)
				     result)]
		   (redirect-recur redirect pipeline initial-value (inc err-count)))))))))
     result))


;;;

(defn wait-for-result
  "Waits for a result-channel to emit a result.  If it succeeds, returns the result.
   If there was an error, the exception is re-thrown.

   If the timeout elapses, a java.util.concurrent.TimeoutException is thrown."
  ([result-channel]
     (wait-for-result result-channel -1))
  ([^ResultChannel result-channel timeout]
     (let [value (promise)]
       (receive (poll-result result-channel timeout)
	 #(deliver value %))
       (let [value @value]
	 (if (nil? value)
	   (throw (TimeoutException. "Timed out waiting for result from pipeline."))
	   (let [[k result] value]
	     (case k
	       :error (throw result)
	       :success result)))))))

(defn siphon-result
  "Siphons the result from one result-channel to another."
  [src dst]
  (on-success src #(success! dst %))
  (on-error src #(error! dst %))
  src)

;;;

(defn- get-opts [opts+rest]
  (if (-> opts+rest first keyword?)
    (concat (take 2 opts+rest) (get-opts (drop 2 opts+rest)))
    nil))

(defn ^ResultChannel pipeline
  "Returns a function with an arity of one.  Invoking the function will return
   a result channel.

   Stages should either be pipelines, or functions with an arity of one.  These functions
   should either return a result channel, a redirect signal, or a value which will be passed
   into the next stage."
  [& opts+stages]
  (let [opts (apply hash-map (get-opts opts+stages))
	stages (drop (* 2 (count opts)) opts+stages)
	executor (or (:executor opts) *current-executor*)
	pipeline {:stages stages
		  :error-handler (:error-handler opts)
		  :executor executor}
	pipeline-fn (fn [val]
		      (start-pipeline
			(update-in pipeline [:error-handler]
			  #(or %
			     (fn [ex]
			       (when (instance? Throwable ex)
				 (log/warn "lamina.core.pipeline" ex)))))
			val))]
    (when-not (every? ifn? stages)
      (throw (Exception. "Every stage in a pipeline must be a function.")))
    ^{:pipeline pipeline}
    (fn [x]
      (if executor
	(let [result (result-channel)
	      bindings (get-thread-bindings)]
	  (with-executor* executor
	    (with-bindings bindings
	      (siphon-result (pipeline-fn x) result)))
	  result)
	(pipeline-fn x)))))

(defn complete
  "Skips to the end of the inner-most pipeline, causing it to emit 'result'."
  [result]
  (redirect
    (pipeline
      (fn [_]
	(let [ch (result-channel)]
	  (success! ch result))
	result))
    nil))

(defn ^ResultChannel run-pipeline
  "Equivalent to ((pipeline opts+stages) initial-value).

   Returns a pipeline future."
  [initial-value & opts+stages]
  ((apply pipeline opts+stages) initial-value))

(defn ^ResultChannel read-channel
  "For reading channels within pipelines.  Takes a simple channel, and returns
   a result channel representing the next message from the channel.  If the timeout
   elapses, the result channel will emit an error."
  ([ch]
     (read-channel ch -1))
  ([ch timeout]
     (if (drained? ch)
       (throw (Exception. "Cannot read from a drained channel."))
       (let [msg (dequeue ch ::none)]
	 (if-not (= ::none msg)
	   (success-result msg)
	   (let [result (result-channel)]
	     (receive
	       (poll {:ch ch} timeout)
	       #(if %
		  (success! result
		    (second %))
		  (error! result
		    (TimeoutException. (str "read-channel timed out after " timeout " ms")))))
	     result))))))

(defn read-merge
  "For merging asynchronous reads into a pipeline.

   'read-fn' is a function that takes no parameters and returns a value, which
   can be a pipeline channel representing an asynchronous read.

   'merge-fn' is a function which takes two parameters - the incoming value from
   the pipeline and the value from read-fn - and returns a single value that
   will propagate forward into the pipeline."
  [read-fn merge-fn]
  (fn [input]
    (run-pipeline (read-fn)
      #(merge-fn input %))))

(defn wait-stage
  "Creates a pipeline stage that accepts a value, and emits the same value after 'interval' milliseconds."
  [interval]
  (fn [x]
    (run-pipeline
      (when (pos? interval)
	(read-channel (timed-channel interval)))
      (fn [_] x))))

(defn closed-result [ch]
  (let [result (result-channel)]
    (on-closed ch #(success! result true))
    result))

(defn drained-result [ch]
  (let [result (result-channel)]
    (on-drained ch #(success! result true))
    result))

;;;

(defmethod print-method ResultChannel [ch writer]
  (.write writer (.toString ch)))
