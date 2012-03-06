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
    [lamina.core.utils :only (enqueue)])
  (:require
    [lamina.trace.instrument :as i]
    [lamina.trace.pipeline :as p]
    [lamina.trace.timer :as t]
    [lamina.trace.utils :as u]
    [lamina.trace.probe :as pr]))

(import-fn i/instrument)
(import-macro i/instrumented-fn)
(import-macro i/defn-instrumented)

(import-fn t/format-timing)
(import-macro u/time*)
(import-macro u/with-instrumented-pipelines)
(import-macro u/with-instrumentation)

(import-fn pr/canonical-probe-name)
(import-fn pr/probe-channel)
(import-fn pr/error-probe-channel)
(import-fn pr/sympathetic-probe-channel)
(import-fn pr/probe-enabled?)
(import-fn pr/probe-result)
(import-fn pr/select-probes)
(import-fn pr/probe-names)

(defmacro trace* [probe & body]
  (let [probe (if (keyword? probe)
                (name probe)
                probe)]
    `(let [probe-channel# (probe-channel ~probe)]
       (if (probe-enabled? probe-channel#)
         (do
           (enqueue probe-channel# (do ~@body))
           true)
         false))))

(defmacro trace [probe & body]
  (when-not (or (keyword? probe) (string? probe))
    (println
      (str
        (ns-name *ns*) ", line " (-> &form meta :line) ": '"
        (pr-str probe) "' cannot be resolved to a static probe. "
        "This will work, but may cause performance issues. Use trace* to hide this warning.")))
  `(trace* ~probe ~@body))
