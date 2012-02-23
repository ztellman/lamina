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
(import-fn u/shutdown)

(defmacro task [& body]
  `(u/execute default-executor nil (fn [] ~@body) nil))

(defn executor-channel [& {:keys [name executor probes] :as options}]
  (when-not (and name executor)
    (throw (IllegalArgumentException. "executor-channel must be given a :name and :executor")))
  (let [receiver (channel)
        emitter (channel)
        callback (apply trace/instrument #(enqueue emitter %) (apply concat options))]
    (bridge-join receiver name callback emitter)
    (splice emitter receiver)))
