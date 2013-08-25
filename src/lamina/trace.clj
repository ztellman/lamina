;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace
  (:use
    [potemkin]
    [lamina.core])
  (:require
    [lamina.cache :as c]
    [lamina.query :as q]
    [lamina.time :as time]
    [lamina.stats :as stats]
    [lamina.trace.router :as r]
    [lamina.trace.context :as ctx]
    [lamina.trace.instrument :as i]
    [lamina.trace.timer :as t]
    [lamina.trace.utils :as u]
    [lamina.trace.probe :as pr]))

(import-fn ctx/register-context-builder)

(import-fn i/instrument)
(import-macro i/instrumented-fn)
(import-macro i/defn-instrumented)

(import-fn t/distill-timing)
(import-fn t/merge-distilled-timings)
(import-fn t/format-timing)
(import-macro u/time*)
(import-macro u/with-instrumentation)
(import-fn t/add-sub-timing)
(import-fn t/add-to-last-sub-timing)

(import-fn c/subscribe)

(defn tracing?
  "Returns true when called inside an active function trace scope, false otherwise."
  []
  (boolean (lamina.core.context/timer)))

(defn analyze-timings
  "Aggregates timings, and periodically emits statistical information about them."
  [{:keys [period  window]
    :or {window (time/hours 1)
         period (time/period)}
    :as options}
   ch]
  (->> ch

    ;; normalize the input
    (map* distill-timing)

    (q/query-stream
      '[(group-by :task
          [(zip
             {:duration-quantiles [:durations concat moving-quantiles]
              :calls [:durations concat rate rolling-sum]
              :total-duration [:durations concat rolling-sum]
              :sub-tasks [:sub-tasks concat recur]
              })])]
      options)))

;;;

(import-vars
  [lamina.trace.probe

   on-enabled-changed
   on-new-probe
   canonical-probe-name
   probe-channel
   error-probe-channel
   probe-enabled?
   select-probes
   probe-names]

  [lamina.trace.timer

   add-to-trace-counter
   increment-trace-counter])

(defmacro trace*
  "A variant of trace that allows the probe name to be resolved at runtime."
  [probe & body]
  (let [probe (if (keyword? probe)
                (name probe)
                probe)]
    `(let [probe-channel# (probe-channel ~probe)]
       (if (probe-enabled? probe-channel#)
         (do
           (enqueue probe-channel# (do ~@body))
           true)
         false))))

(defmacro trace
  "Enqueues a value into the probe-channel described by `probe`.  The body is executed only
   if there is a consumer for the probe channel; this is essentially a log statement that is
   only active if someone is paying attention.

   For performance reasons, the probe name must be something that can be resolved at compile-time."
  [probe & body]
  (when-not (or (keyword? probe) (string? probe))
    (println
      (str
        (ns-name *ns*) ", line " (-> &form meta :line) ": '"
        (pr-str probe) "' cannot be resolved to a static probe. "
        "This will work, but may cause performance issues. Use trace* to hide this warning.")))
  `(trace* ~probe ~@body))

;;;


(import-fn r/trace-router)
(import-fn r/local-trace-router)
(import-fn r/aggregating-trace-router)

