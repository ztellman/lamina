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
     TimeoutException]))

;;;

(def *inside-pipeline?* false)

;;;

(defrecord ResultChannel [success error]
  Object
  (toString [_]
    (str {:success success, :error error})))

(defn result-channel []
  (ResultChannel. (constant-channel) (constant-channel)))

(defn error-result [val]
  (ResultChannel. nil-channel (constant-channel val)))

(defn success-result [val]
  (ResultChannel. (constant-channel val) nil-channel))

(defn result-channel? [x]
  (instance? ResultChannel x))

;;;

(defrecord Redirect [pipeline value])

(defn redirect
  ([pipeline]
     (Redirect. pipeline ::initial))
  ([pipeline value]
     (Redirect. pipeline value)))

(defn redirect? [x]
  (instance? Redirect x))

(defn restart
  ([]
     (restart ::initial))
  ([value]
     (redirect ::pipeline value)))

;;;

(defn handle-error [pipeline ^ResultChannel result ^ResultChannel outer-result]
  (let [[_ ex :as err] (dequeue (.error result) nil)]
    (if-let [redirect (if-let [handler (:error-handler pipeline)]
			(let [result (apply handler err)]
			  (when (redirect? result)
			    result)))]
      redirect
      (do
	(enqueue (.error outer-result) err)
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
	   (enqueue (.error result)
	     [initial-value (Exception. "Error loop detected in pipeline.")])
	   
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
	       (receive (poll ch)
		 (fn [[outcome value]]
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
				result))))))
	   
	   (empty? fns)
	   (enqueue (.success result) value)
	   
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
				     (error-result [initial-value val])
				     result)]
		   (redirect-recur redirect pipeline initial-value (inc err-count)))))))))
     result))


;;;

(defn- get-opts [opts+rest]
  (if (-> opts+rest first keyword?)
    (concat (take 2 opts+rest) (get-opts (drop 2 opts+rest)))
    nil))

(defn pipeline
  "Returns a function with an arity of one.  Invoking the function will return
   a pipeline channel.

   Stages should either be pipelines, or functions with an arity of one.  These functions
   should either return a pipeline channel, a redirect signal, or a value which will be passed
   into the next stage."
  [& opts+stages]
  (let [opts (apply hash-map (get-opts opts+stages))
	stages (drop (* 2 (count opts)) opts+stages)
	pipeline {:stages stages
		  :error-handler (:error-handler opts)}]
    (when-not (every? fn? stages)
      (throw (Exception. "Every stage in a pipeline must be a function.")))
    ^{:pipeline pipeline}
    (fn [x]
      (start-pipeline
	(update-in pipeline [:error-handler]
	  #(or %
	     (when-not *inside-pipeline?*
	       (fn [val ex]
		 (when (instance? Throwable ex)
		   (log/error "lamina.core.pipeline" ex))))))
	x))))

(defn complete
  "Short-circuits the inner-most pipeline, returning the result."
  [result]
  (redirect
    (pipeline
      (fn [_]
	(let [ch (result-channel)]
	  (enqueue (:success ch) result))
	result))
    nil))

(defn run-pipeline
  "Equivalent to ((pipeline opts+stages) initial-value).

   Returns a pipeline future."
  [initial-value & opts+stages]
  ((apply pipeline opts+stages) initial-value))

(defn blocking
  "Takes a synchronous function, and returns a function which will be executed asynchronously,
   and whose invocation will return a pipeline channel."
  [f]
  (fn [x]
    (let [result (result-channel)
	  {success :success error :error} result]
      (future
	(try
	  (enqueue success (f x))
	  (catch Exception e
	    (enqueue error [x e]))))
      result)))

(defn read-channel
  "For reading channels within pipelines.  Takes a simple channel, and returns
   a pipeline channel."
  ([ch]
     (read-channel ch -1))
  ([ch timeout]
     (if (closed? ch)
       (throw (Exception. "Cannot read from a closed channel."))
       (let [msg (dequeue ch ::none)]
	 (if-not (= ::none msg)
	   msg
	   (let [result (result-channel)
		 {success :success error :error} result]
	     (receive
	       (poll {:ch ch} timeout)
	       #(if %
		  (enqueue success
		    (second %))
		  (enqueue error
		    [nil (TimeoutException. (str "read-channel timed out after " timeout " ms"))])))
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

;;;

(defn wait-for-result
  "Waits for a pipeline to complete.  If it succeeds, returns the result.
   If there was an error, the exception is re-thrown."
  ([result-channel]
     (wait-for-result result-channel -1))
  ([result-channel timeout]
     (let [value (promise)]
       (receive (poll result-channel timeout)
	 #(deliver value %))
       (let [value @value]
	 (if (nil? value)
	   (throw (TimeoutException. "Timed out waiting for result from pipeline."))
	   (let [[k result] value]
	     (case k
	       :error (throw (second result))
	       :success result)))))))

(defn siphon-result
  [src dst]
  (receive (:success src) #(enqueue (:success dst) %))
  (receive (:error src) #(enqueue (:error dst) %)))

(defmethod print-method ResultChannel [ch writer]
  (.write writer (str ch)))
