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
  (let [explicit-name? (string? (first body))
        name (if explicit-name? (first body) "task")
        body (if explicit-name? (rest body) body)]
    `((trace/instrumented-fn
        task
        {:executor default-executor}
        []
        ~@body))))

(defmacro bound-task
  "Executes the body on a separate thread, returning an unrealized result representing the eventual
   value or error.  Unlike `task`, thread-local bindings are preserved when evaluating the body."
  [& body]
  (let [explicit-name? (string? (first body))
        name (if explicit-name? (first body) "bound-task")
        body (if explicit-name? (rest body) body)]
    `((trace/instrumented-fn
        ~name
        {:with-bindings? true
         :executor default-executor}
        []
        ~@body))))

(defn executor-channel
  "Creates a channel that ensures all downstream channels will receive messages on the thread-pool
   specified by :executor.  This can be useful for both rate-limiting and parallelization.

   This may change the downstream order of messages."
  [{:keys [name executor probes] :as options}]
  (when-not (and name executor)
    (throw (IllegalArgumentException. "executor-channel must be given a :name and :executor")))
  (let [receiver (channel)
        emitter (channel)
        pending (atom 0)
        callback (trace/instrument
                   (fn [msg]
                     (let [result (enqueue emitter msg)]

                       ;; check if no more messages are coming, and if so, close the emitter
                       (when (and
                               (zero? (swap! pending dec))
                               (drained? receiver))
                         (close emitter))

                       result))
                   options)]

    ;; if the receiver is closed when there are no pending messages, close immediately
    (on-drained receiver
      #(when (zero? @pending)
         (close emitter)))
    
    (bridge-siphon receiver emitter name
      (fn [msg]
        (swap! pending inc)
        (callback msg)))
    
    (splice emitter receiver)))

(defn defer
  "Defers propagation of messages onto another thread pool. This may change the downstream
   order of messages."
  ([ch]
     (defer default-executor ch))
  ([executor ch]
     (let [ex (executor-channel {:name "defer", :executor executor, :capture :none})]
       (join ch ex)
       ex)))

(defn pmap*
  "Like map*, but executes function on thread pool. This preserves the downstream order of
   messages."
  ([f ch]
     (pmap* {:executor default-executor} f ch))
  ([{:keys [executor name]
     :as options
     :or {executor default-executor
          name "pmap*"}}
    f ch]
     (->> ch
       (map* (trace/instrument f
               (merge
                 {:executor executor, :name name}
                 options)))
       emit-in-order)))

