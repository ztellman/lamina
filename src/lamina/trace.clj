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
    [lamina.trace.core :as trace]))

;;;

(trace/def-log-channel log-trace :trace)
(trace/def-log-channel log-debug :debug)
(trace/def-log-channel log-info :info)
(trace/def-log-channel log-warn :warn)
(trace/def-log-channel log-error :error)
(trace/def-log-channel log-fatal :fatal)

;;;

(import-fn #'trace/register-probe)
(import-fn #'trace/canonical-probe)
(import-fn #'trace/on-new-probe)
(import-fn #'trace/probe-channel)

(defn registered-probes []
  @trace/registered-probes)

(defmacro trace
  "Enqueues the value into a probe-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [probe & body]
  (apply trace/expand-trace probe body))

(defmacro trace*
  [canonical-probe & body]
  (apply trace/expand-trace* canonical-probe body))

(defmacro trace->> [probe & forms]
  (apply trace/expand-trace->> probe forms))

;;;

(defn siphon-probes [prefix m]
  (doseq [[k v] m]
    (siphon (probe-channel [prefix k]) {v identity})))

(defn- instrument-calls [args result start options]
  (trace [(:name options) :calls]
    (let [end (System/nanoTime)]
      {:args args
       :result result
       :start-time start
       :end-time end
       :duration (/ (- end start) 1e6)})))

(defn- instrument-errors [args start options]
  (fn [ex]
    (trace [(:name options) :errors]
      (let [end (System/nanoTime)]
	{:args args
	 :exception ex
	 :start-time start
	 :end-time end
	 :duration (/ (- end start) 1e6)}))))

(defn trace-wrap [f options]
  (when-not (:name options)
    (throw (Exception. "Must define :name for instrumented function.")))
  (siphon-probes (:name options) (:probes options))
  (fn [& args]
    (let [start-time (System/nanoTime)
	  result (run-pipeline (apply f args) :error-handler (fn [_]))]
      (run-pipeline result
	:error-handler (instrument-errors args start-time options)
	#(instrument-calls args % start-time options))
      result)))

(defmacro defn-trace [name & forms]
  (let [options (->> forms
		  (take-while (complement vector?))
		  (filter map?)
		  first)]
    `(do
       (defn ~name ~@forms)
       (def ~name (trace-wrap ~name (assoc ~options :name ~(str name)))))))
