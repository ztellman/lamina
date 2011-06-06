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
    [clojure.contrib.core :only (dissoc-in)])
  (:require
    [clojure.string :as str]
    [lamina.core.queue :as q]
    [lamina.core.observable :as o]
    [clojure.contrib.logging :as log]))

;;;

(def probe-channels (ref {}))
(def enabled-probe-channels (atom {}))
(def registered-probes (atom {}))
(def *probe-prefix* "")

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
  (if (coll? probe)
    (mapcat canonical-probe probe)
    (str/split
      (if (keyword? probe)
	(name probe)
	(str probe))
      #"[.]")))

(defn register-probe [probe]
  (swap! registered-probes update-in probe #(or % {})))

(defn constant-probe? [probe]
  (or
    (string? probe)
    (keyword? probe)
    (and (coll? probe) (every? constant-probe? probe))))

(defn- probe-channel- [probe]
  (let [ch (channel)]
    (o/subscribe (-> ch queue q/distributor)
      {::probe
       (o/observer
	 (fn [_] )
	 nil
	 (fn [observers]
	   (if-not (= 3 (count observers))
	     (swap! enabled-probe-channels update-in probe #(or % {}))
	     (swap! enabled-probe-channels dissoc-in probe))))})
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

;;;

(defn expand-trace
  "Enqueues the value into a probe-channel only if there's a consumer for it.  If there
   is no consumer, the body will not be evaluated."
  [probe & body]
  (when (constant-probe? probe)
    (register-probe (canonical-probe probe)))
  (let [probe-sym (gensym "probe")]
    `(let [~probe-sym (canonical-probe ~probe)]
       ~@(when-not (constant-probe? probe)
	   `((register-probe ~probe-sym)))
       (when-not (= ::empty (get-in @enabled-probe-channels ~probe-sym ::empty))
	 (enqueue (probe-channel ~probe-sym) (do ~@body))
	 nil))))

(defn expand-trace->> [probe & forms]
  (let [ch-sym (gensym "ch")
	dests? (vector? (last forms))
	probe-sym (gensym "probe")]
    `(let [~ch-sym (channel)
	   ~probe-sym (concat *probe-prefix* (canonical-probe ~probe))]
       (register-probe ~probe-sym)
       (binding [*probe-prefix* ~probe-sym]
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
		 (probe-channel ~probe-sym))
	       (repeat identity)))))
       (let [ch# (channel)]
	 (receive-all ch#
	   (fn [msg#]
	     (when-not (= ::empty (get-in @enabled-probe-channels ~probe-sym ::empty))
	       (enqueue ~ch-sym msg#))))
	 ch#))))
