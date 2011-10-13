;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.core
  (:use
    [lamina.core channel seq]
    [clojure.core.incubator :only (dissoc-in)])
  (:require
    [clojure.string :as str]
    [lamina.core.queue :as q]
    [lamina.core.observable :as o]
    [clojure.tools.logging :as log]))

;;;

(def probe-channels (ref {}))
(def probe-switches (atom {}))
(def ^{:dynamic true} *probe-prefix* nil)

(def new-probe-publisher (channel))
(receive-all new-probe-publisher (fn [_] ))

;;;

(defn logger [level]
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
     (lamina.core.seq/receive-all ~channel-name (logger ~level))))

;;;

(defn canonical-probe [probe]
  (cond
    (keyword? probe) probe
    (string? probe) (keyword probe)
    (coll? probe) (->> probe
		    (map canonical-probe)
		    (map name)
		    (interpose ":")
		    (apply str)
		    keyword)
    :else (throw (Exception. (str "Can't convert " (pr-str probe) " to probe")))))

(defn register-probe [& probes]
  (doseq [probe probes]
    (swap! probe-switches update-in [probe] #(or % (atom false)))))

(defn probe-switch [probe]
  (let [switch (get @probe-switches probe ::none)]
    (if-not (= ::none switch)
      switch
      (do
	(register-probe probe)
	(get @probe-switches probe)))))

(defn on-new-probe [& callbacks]
  (apply receive-all new-probe-publisher callbacks))

(defn constant-probe? [probe]
  (or
    (string? probe)
    (keyword? probe)
    (and (coll? probe) (every? constant-probe? probe))))

(defn- probe-channel- [probe]
  (let [ch (channel)
	switch (probe-switch probe)]
    (o/subscribe (-> ch queue q/distributor)
      {::probe
       (o/observer
	 (fn [_] )
	 nil
	 (fn [observers]
	   (reset! switch (not= 3 (count observers)))))})
    ch))

(defn probe-channel
  "Returns a channel corresponding to the probe which will track if there are any
   consumers."
  [probe]
  (let [probe (canonical-probe probe)]
    (if-let [ch (get @probe-channels probe)]
      ch
      (dosync
	(let [channels (ensure probe-channels)]
	  (if (contains? channels probe)
	    (channels probe)
	    (let [ch (probe-channel- probe)]
	      (alter probe-channels assoc probe ch)
	      ch)))))))

;;;

(defn expand-trace
  "Enqueues the value into a probe-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [probe & body]
  (if (constant-probe? probe)
    (let [probe (canonical-probe probe)
	  ch-sym (gensym "ch")
	  switch-sym (gensym "switch")]
      (eval
	`(do
	   (def ~ch-sym (probe-channel ~probe))
	   (def ~switch-sym (probe-switch ~probe))))
      `(if (deref ~switch-sym)
	 (do
	   (enqueue ~ch-sym (do ~@body))
	   true)
	 false))
    `(let [probe# (canonical-probe ~probe)
	   switch# (probe-switch probe#)]
       (if (deref switch#)
	 (do
	   (enqueue (probe-channel probe#) (do ~@body))
	   true)
	 false))))

(defn expand-trace->> [probe & forms]
  (let [ch-sym (gensym "ch")
	dests? (vector? (last forms))]
    `(let [~ch-sym (channel)
	   probe# (canonical-probe
		    (if *probe-prefix*
		      [*probe-prefix* ~probe]
		      ~probe))]
       (binding [*probe-prefix* probe#]
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
		 (probe-channel probe#))
	       (repeat identity)))))
       (let [ch# (channel)
	     switch# (probe-switch probe#)]
	 (receive-all ch#
	   (fn [msg#]
	     (when (deref switch#)
	       (enqueue ~ch-sym msg#))))
	 ch#))))
