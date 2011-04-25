;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.logging
  (:use
    [lamina.core channel seq pipeline])
  (:require
    [clojure.contrib.logging :as log])
  (:import
    [java.util.concurrent
     TimeoutException]))

(defn logger [level]
  #(if (instance? Throwable %)
     (log/log level nil %)
     (log/log level (str %))))

(defmacro def-log-channel [name level]
  `(do
     (def ~name (channel))
     (receive-all ~name (logger ~level))))

(def-log-channel log-trace :trace)
(def-log-channel log-debug :debug)
(def-log-channel log-info :info)
(def-log-channel log-warn :warn)
(def-log-channel log-error :error)
(def-log-channel log-fatal :fatal)

;;;

(defmacro siphon->> [& forms]
  `(let [ch# (channel)]
     (siphon (->> ch# ~@(butlast forms)) {~(last forms) identity})
     ch#))

(def default-timeout-handler
  (let [ch (channel)]
    (receive-all ch
      (fn [[^Thread thread result timeout]]
	(error! result (TimeoutException. (str "Timed out after " timeout "ms.")))
	(.interrupt thread)))
    ch))

;;;

(defn rate-limit [period ch]
  (let [ch* (channel)
	val (atom ::none)]
    (receive-all ch
      #(when-not (and (drained? ch) (nil? %))
	 (reset! val %)))
    (run-pipeline nil
      (wait-stage period)
      (fn [_]
	(let [val @val]
	  (when-not (= val ::none)
	    (enqueue ch* val))))
      (fn [_]
	(if-not (drained? ch)
	  (restart)
	  (close ch*))))
    ch*))
