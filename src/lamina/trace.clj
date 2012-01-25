;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace
  (:use
    [lamina.core channel seq pipeline]
    potemkin)
  (:require
    [clojure.tools.logging :as log]
    [lamina.trace.core :as trace]))

;;;

(trace/def-log-channel log-trace :trace)
(trace/def-log-channel log-debug :debug)
(trace/def-log-channel log-info :info)
(trace/def-log-channel log-warn :warn)
(trace/def-log-channel log-error :error)
(trace/def-log-channel log-fatal :fatal)

;;;

(import-fn trace/register-probe)
(import-fn trace/canonical-probe)
(import-fn trace/on-new-probe)
(import-fn trace/probe-channel)

(defn registered-probes []
  @trace/probe-switches)

(defmacro trace
  "Enqueues the value into a probe-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [probe & body]
  (apply trace/expand-trace probe body))

(defmacro trace->> [probe & forms]
  (apply trace/expand-trace->> probe forms))

;;;

(defn siphon-probes [prefix m]
  (doseq [[k v] m]
    (siphon (probe-channel [prefix k]) {v identity})))

(defn- call-tracer [options]
  (let [probe (canonical-probe [(:name options) :calls])
	args-transform (if-let [transform-fn (:args-transform options)]
			transform-fn
			identity)]
    (fn [args]
      (trace probe (args-transform args)))))

(defn- result-tracer [options]
  (let [probe (canonical-probe [(:name options) :results])
	args-transform (if-let [transform-fn (:args-transform options)]
			transform-fn
			identity)
	result-transform (if-let [transform-fn (:result-transform options)]
			   transform-fn
			   identity)]
    (fn [args result start]
      (trace probe
	(let [end (System/nanoTime)]
	  {:args (args-transform args)
	   :result (result-transform result)
	   :start-time (/ start 1e6)
	   :end-time (/ end 1e6)
	   :duration (/ (- end start) 1e6)})))))

(defn- error-tracer [options]
  (let [probe (canonical-probe [(:name options) :errors])
	args-transform (if-let [transform-fn (:args-transform options)]
			 transform-fn
			 identity)]
    (fn [args start ex]
      (when-not (trace probe
                  (let [end (System/nanoTime)]
                    {:args (args-transform args)
                     :exception ex
                     :start-time (/ start 1e6)
                     :end-time (/ end 1e6)
                     :duration (/ (- end start) 1e6)}))
        (log/error ex (str "Unconsumed error on " probe))))))

(defn trace-wrap [f options]
  (when-not (:name options)
    (throw (Exception. "Must define :name for instrumented function.")))
  (siphon-probes (:name options) (:probes options))
  (let [calls (call-tracer options)
	results (result-tracer options)
	errors (error-tracer options)]
    (fn [& args]
      (calls args)
      (let [start-time (System/nanoTime)]
	(try
	  (let [result (apply f args)]
	    (run-pipeline result
	      :error-handler #(errors args start-time %)
	      #(results args % start-time))
	    result)
	  (catch Exception e
	    (errors args start-time e)
	    (throw e)))))))

(defmacro defn-trace [name & forms]
  (let [options (->> forms
		  (take-while #(or (symbol? %) (string? %) (map? %)))
		  (filter map?)
		  first)]
    `(do
       (defn ~name ~@forms)
       (def ~name
	 (trace-wrap ~name
	   (merge
	     {:name (str (str *ns*) ":" ~name)}
	     ~(or options {})))))))
