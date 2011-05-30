;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace
  (:use
    [lamina.core channel seq pipeline])
  (:require
    [clojure.string :as str]
    [lamina.core.queue :as q]
    [lamina.core.observable :as o]
    [clojure.contrib.logging :as log]))

;;;

(defn- logger [level]
  #(if (instance? Throwable %)
     (log/log level nil %)
     (log/log level (str %))))

(defmacro def-log-channel [channel-name level]
  `(do
     (def ~(with-meta
	    channel-name
	    {:doc (str "Every message enqueued into this channel will be logged at the "
		    (str/upper-case (name level))
		    " level.")})
       (channel))
     (receive-all ~channel-name (logger ~level))))

(def-log-channel log-trace :trace)
(def-log-channel log-debug :debug)
(def-log-channel log-info :info)
(def-log-channel log-warn :warn)
(def-log-channel log-error :error)
(def-log-channel log-fatal :fatal)

;;;

(defn sample-every
  "Returns a channel which will emit the last message enqueued into 'ch' every 'period'
   milliseconds."
  [period ch]
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

;;;

(def trace-channels (ref {}))
(def enabled-trace-channels (ref #{}))
(def *trace-prefix* "")

(defn canonical-trace-key [trace-key]
  (if (keyword? trace-key)
    (name trace-key)
    (str trace-key)))

(defn- trace-channel- [trace-key]
  (let [ch (channel)]
    (o/subscribe (-> ch queue q/distributor)
      {::sink
       (o/observer
	 (fn [msg]
	   )
	 nil
	 (fn [observers]
	   (dosync
	     (alter enabled-trace-channels
	       (if (= 3 (count observers))
		 disj
		 conj)
	       trace-key))))})
    ch))

(defn trace-channel
  "Returns a channel corresponding to the trace-key which will track if there are any
   consumers."
  [trace-key]
  (let [trace-key (canonical-trace-key trace-key)]
    (dosync
      (let [channels (ensure trace-channels)]
	(if (contains? channels trace-key)
	  (channels trace-key)
	  (let [ch (trace-channel- trace-key)]
	    (alter trace-channels assoc trace-key ch)
	    ch))))))

(defmacro trace
  "Enqueues the value into a trace-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [trace-key & body]
  `(let [key# (canonical-trace-key ~trace-key)]
     (when (contains? @lamina.trace/enabled-trace-channels key#)
       (enqueue (trace-channel key#) (do ~@body)))))

(defmacro trace->> [trace-key & forms]
  (let [ch-sym (gensym "ch")
	dests? (vector? (last forms))]
    `(let [~ch-sym (channel)
	   key# (str
		  *trace-prefix*
		  (when-not (empty? *trace-prefix*) ".")
		  (canonical-trace-key ~trace-key))]
       (binding [*trace-prefix* key#]
	 (lamina.core.seq/siphon
	   ~(let [operators (if dests? (butlast forms) forms)]
	      (if (empty? operators)
		ch-sym
		`(->> ~ch-sym ~@operators)))
	   (let [dsts# ~(when dests? (last forms))]
	     (zipmap
	       (conj
		 (cond
		   (nil? dsts#) []
		   (coll? dsts#) (vec dsts#)
		   :else [dsts#])
		 (trace-channel key#))
	       (repeat identity)))))
       (let [ch# (channel)]
	 (receive-all ch#
	   (fn [msg#]
	     (when (contains? @lamina.trace/enabled-trace-channels key#)
	       (enqueue ~ch-sym msg#))))
	 ch#))))
