;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor
  (:use
    [potemkin]
    [lamina core api])
  (:require
    [lamina.executor.core :as c]
    [lamina.executor.utils :as u]
    [lamina.trace.timer :as t]
    [lamina.trace :as trace]))

(import-fn c/executor)
(import-fn c/default-executor)
(import-macro c/defexecutor)
(import-fn u/shutdown)

(defmacro task
  "Executes the body on a separate thread, returning an unrealized result representing the eventual
   value or error."
  [& body]
  `((trace/instrumented-fn
      {:name "task"
       :executor default-executor}
      []
      ~@body)))

(defmacro bound-task
  "Executes the body on a separate thread, returning an unrealized result representing the eventual
   value or error.  Unlike 'task', thread-local bindings are preserved when evaluating the body."
  [& body]
  `((trace/instrumented-fn
      {:name "bound-task"
       :executor default-executor
       :with-bindings? true}
      []
      ~@body)))

(defn executor-channel
  "Creates a channel that ensures all downstream channels will receive messages on the thread-pool
   specified by :executor.  This can be useful for both rate-limiting and parallelization."
  [& {:keys [name executor probes] :as options}]
  (when-not (and name executor)
    (throw (IllegalArgumentException. "executor-channel must be given a :name and :executor")))
  (let [receiver (channel)
        emitter (channel)
        callback (apply trace/instrument #(enqueue emitter %) (apply concat options))]
    (bridge-join receiver name callback emitter)
    (splice emitter receiver)))
