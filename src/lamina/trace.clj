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

(def probe-channels (ref {}))
(def enabled-probe-channels (ref #{}))
(def *probe-prefix* "")

(defn canonical-probe [probe]
  (if (coll? probe)
    (mapcat canonical-probe probe)
    (str/split
      (if (keyword? probe)
	(name probe)
	(str probe))
      #"[.]")))

(defn- probe-channel- [probe]
  (let [ch (channel)]
    (o/subscribe (-> ch queue q/distributor)
      {::probe
       (o/observer
	 (fn [_] )
	 nil
	 (fn [observers]
	   (dosync
	     (alter enabled-probe-channels
	       (if (= 3 (count observers))
		 disj
		 conj)
	       probe))))})
    ch))

(defn probe-channel
  "Returns a channel corresponding to the probe which will track if there are any
   consumers."
  [probe]
  (let [probe (canonical-probe probe)]
    (dosync
      (let [channels (ensure probe-channels)]
	(if (contains? channels probe)
	  (channels probe)
	  (let [ch (probe-channel- probe)]
	    (alter probe-channels assoc probe ch)
	    ch))))))

(defmacro trace
  "Enqueues the value into a probe-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [probe & body]
  `(let [key# (canonical-probe ~probe)]
     (when (contains? @lamina.trace/enabled-probe-channels key#)
       (enqueue (probe-channel key#) (do ~@body)))))

(defmacro trace->> [probe & forms]
  (let [ch-sym (gensym "ch")
	dests? (vector? (last forms))]
    `(let [~ch-sym (channel)
	   key# (str
		  *probe-prefix*
		  (when-not (empty? *probe-prefix*) ".")
		  (canonical-probe ~probe))]
       (binding [*probe-prefix* key#]
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
		 (probe-channel key#))
	       (repeat identity)))))
       (let [ch# (channel)]
	 (receive-all ch#
	   (fn [msg#]
	     (when (contains? @lamina.trace/enabled-probe-channels key#)
	       (enqueue ~ch-sym msg#))))
	 ch#))))

;;;

(defn- instrument-timing [args result start end options]
  (let [timing (delay
		 {:args args
		  :result result
		  :start-time start
		  :end-time end
		  :duration (/ (- end start) 1e6)})]
    (when-let [timing-hook (-> options :probes :timing)]
      (enqueue timing-hook @timing))
    (trace [(:name options) :timing]
      @timing)))

(defn trace-fn [f options]
  (when-not (:name options)
    (throw (Exception. "Must define :name for instrumented function.")))
  (fn [& args]
    (let [start-time (System/nanoTime)
	  result (run-pipeline (apply f args))]
      (run-pipeline result #(instrument-timing args % start-time (System/nanoTime) options))
      result)))

(defmacro defn-trace [name & forms]
  (let [options (->> forms
		  (take-while (complement vector?))
		  (filter map?)
		  first)]
    `(do
       (defn ~name ~@forms)
       (def ~name (trace-fn ~name (assoc ~options :name ~(str name)))))))
