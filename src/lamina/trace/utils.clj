(ns lamina.trace.utils
  (:use
    [lamina.core])
  (:require
    [lamina.trace.probe :as pr]
    [lamina.trace.pipeline :as p]
    [lamina.trace.timer :as t]
    [lamina.core.context :as context])
  (:import
    [java.io Writer]))

(defmacro with-instrumented-pipelines
  "Returns detailed data for all pipelines executed within the scope."
  [& body]
  `(let [tracer# (p/root-pipeline-tracer nil nil)]
     (context/with-context (context/assoc-context :pipeline-tracer tracer#)
       @(run-pipeline nil
          (fn [_#]
            ~@body)))
     (-> @tracer# :sub-tasks first :sub-tasks)))

;; factored out into functions to aid JIT
(defn print-timing-result [r]
  (let [^Writer out *out*]
    (run-pipeline r
      {:error-handler (fn [_])}
      #(do
         (.write out (t/format-timing %))
         (.write out "\n")
         (.flush out)))))

(defn capture-timings
  [description probe-channel f]
  (let [timer (t/timer description nil probe-channel probe-channel false)
        unwrap? (atom true)
        result (context/with-context (context/assoc-context :timer timer)
                 (run-pipeline nil
                   {:error-handler (fn [ex] (t/mark-error timer ex))}
                   (fn [_]
                     (let [result (f)]
                       (reset! unwrap? (not (result-channel? result)))
                       result))
                   (fn [result]
                     (t/mark-return timer result)
                     result)))]
    (if @unwrap?
      @result
      result)))

(defmacro with-instrumentation
  "Returns the full timing data for all code called within the scope."
  [& body]
  `(let [result# (result-channel)]
     (capture-timings "with-instrumentation" (pr/probe-result result#)
       (fn [] ~@body))
     @result#))

(defmacro time*
  "A somewhat more useful variant of (time ...), which captures the sub-timings of all instrumented functions
   called within the scope.  If the body returns an unrealized value, time* will wait for it to become realized."
  [& body]
  `(let [result# (result-channel)]
     (print-timing-result result#)
     (capture-timings "time" (pr/probe-result result#)
       (fn [] ~@body))))

