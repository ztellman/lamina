(ns lamina.trace.utils
  (:use
    [lamina.core])
  (:require
    [lamina.trace.pipeline :as p]
    [lamina.trace.timer :as t]
    [lamina.core.context :as context])
  (:import
    [java.io Writer]))

(defmacro trace-pipelines [& body]
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

(defn capture-timing
  [probe-channel f]
  (let [timer (t/timer "time*" nil probe-channel false)
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

(defmacro time*
  [& body]
  `(let [result# (result-channel)]
     (print-timing-result result#)
     (capture-timing (probe-result result#)
       (fn [] ~@body))))

