;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.queue
  (:use
    [clojure walk set])
  (:require
    [lamina.core.observable :as o])
  (:import
    [lamina.core.observable ConstantObservable]))

;;;

(defprotocol EventQueueProto
  (source [this])
  (distributor [this])
  (enqueue [this msgs])
  (pop- [this])
  (receive- [this callback])
  (listen [this callbacks])
  (on-close [this callbacks])
  (closed? [this])
  (cancel-callbacks [this callbacks]))

(defn receive [q callbacks]
  (let [msg (pop- q)]
    (if-not (= o/empty-value msg)
      (do
	(doseq [c callbacks]
	  (c msg))
	true)
      (receive- q callbacks))))

(def nil-queue
  (reify EventQueueProto
   (source [_] o/nil-observable)
   (distributor [_] o/nil-observable)
   (enqueue [_ _] false)
   (pop- [_] o/empty-value)
   (receive- [_ _] false)
   (listen [_ _] false)
   (on-close [_ _] false)
   (closed? [_] true)
   (cancel-callbacks [_ _])
   (toString [_] "")))

;;;

(declare gather-receivers)

(declare gather-listeners)

(declare gather-callbacks)

(declare send-to-callbacks)

(defmacro update-and-send [q & body]
  `(if (o/closed? (.source ~q))
     false
     (do
       (send-to-callbacks
	 (dosync
	   ~@body
	   (gather-callbacks (deref (.q ~q)) ~q true)))
       true)))

(deftype EventQueue
  [source distributor q
   receivers listeners close-callbacks
   accumulate]
  EventQueueProto
  (source [_]
    source)
  (distributor [_]
    distributor)
  (enqueue [this msgs]
    (if @accumulate
      (update-and-send this
	(apply alter q conj msgs))
      (send-to-callbacks
	(dosync
	  (gather-callbacks msgs this false)))))
  (on-close [_ callbacks]
    (when (dosync
	    (ensure close-callbacks)
	    (if (o/closed? source)
	      true
	      (do
		(apply alter close-callbacks conj callbacks)
		false)))
      (doseq [c callbacks]
	(c))))
  (pop- [_]
    (if (empty? @q)
      o/empty-value
      (dosync
	(let [msg (first @q)]
	  (alter q pop)
	  msg))))
  (receive- [this callbacks]
    (update-and-send this
      (apply alter receivers conj callbacks)))
  (listen [this callbacks]
    (update-and-send this
      (apply alter listeners conj callbacks)))
  (closed? [_]
    (and (o/closed? source) (empty? @q)))
  (cancel-callbacks [_ callbacks]
    (dosync
      (apply alter listeners disj callbacks)
      (apply alter receivers disj callbacks)))
  clojure.lang.Counted
  (count [_]
    (count @q))
  (toString [_]
    (str (vec @q))))

(defn gather-receivers [^EventQueue q msgs]
  (let [receivers (.receivers q)
	rc @receivers]
    (when-not (or (empty? rc) (empty? msgs))
      (let [msg (first msgs)]
	(alter receivers difference rc)
	[[msg rc]]))))

(defn gather-listeners [^EventQueue q msgs]
  (let [listeners (.listeners q)
	lst @listeners]
    (loop [msgs msgs, l lst, result []]
      (if (empty? msgs)
	(do
	  (alter listeners difference (difference lst l))
	  result)
	(let [msg (first msgs)
	      l (->> l
		  (map #(when-let [[complete? f] (% msg)]
			  [f (when complete? %)]))
		  (remove nil?))]
	  (if (empty? l)
	    (do
	      (alter listeners difference lst)
	      result)
	    (recur
	      (rest msgs)
	      (set (->> l (map second) (remove nil?)))
	      (conj result [msg (map first l)]))))))))

(defn gather-callbacks [msgs ^EventQueue q drop?]
  (let [l (gather-listeners q msgs)
	r (gather-receivers q msgs)
	drop-cnt (max (count l) (count r))]
    (when (and drop? (pos? drop-cnt))
      (let [msgs (.q q)]
	(ensure msgs)
	(ref-set msgs
	  (loop [cnt drop-cnt, msgs @msgs]
	    (if (zero? cnt)
	      msgs
	      (recur (dec cnt) (pop msgs)))))))
    (when (closed? q)
      (doseq [c @(.close-callbacks q)]
	(c))
      (ref-set (.close-callbacks q) nil))
    (concat l r)))

(defn send-to-callbacks [msgs-and-targets]
  (if (= o/empty-value msgs-and-targets)
    false
    (do
      (doseq [[msg callbacks] msgs-and-targets]
	(doseq [c callbacks]
	  (c msg)))
      true)))

(defn setup-accumulate-bit [accumulate q]
  (let [src (source q)
	dst (distributor q)]
    (o/subscribe dst
      {q (o/observer
	      #(enqueue q %)
	      nil
	      #(reset! accumulate (= (set (keys %)) #{src q})))})))

(defn queue
  ([source]
     (queue source nil))
  ([source messages]
     (let [accumulate (atom true)
	   distributor (o/observable)
	   q (EventQueue.
	       source
	       distributor
	       (ref (if messages
		      (apply conj clojure.lang.PersistentQueue/EMPTY messages)
		      clojure.lang.PersistentQueue/EMPTY))
	       (ref #{})
	       (ref #{})
	       (ref #{})
	       accumulate)]
       (o/siphon source distributor)
       (setup-accumulate-bit accumulate q)
       q)))

(defn copy-queue
  [f source ^EventQueue q]
  (let [accumulate (atom true)
	distributor (o/observable)
	copy (EventQueue.
	       source
	       distributor
	       (ref nil)
	       (ref #{})
	       (ref #{})
	       (ref #{})
	       accumulate)]
    (o/siphon source distributor)
    (dosync
      (ensure (.q q))
      (ref-set (.q copy) (f @(.q q)))
      (setup-accumulate-bit accumulate copy)
      copy)))

;;;

(deftype ConstantEventQueue [^ConstantObservable source]
  EventQueueProto
  (source [_] source)
  (distributor [_] source)
  (enqueue [_ _] (assert false))
  (pop- [_] @(.val source))
  (receive- [_ callbacks]
    (o/subscribe source
      (zipmap
	callbacks
	(map
	  (fn [f] (o/observer #(f (first %))))
	  callbacks))))
  (listen [_ callbacks]
    (o/subscribe source
      (zipmap
	callbacks
	(map
	  (fn [f]
	    (o/observer
	      #(let [msg (first %)]
		 (when-let [f* (f msg)]
		   (f* msg)))))
	  callbacks))))
  (on-close [_ callbacks]
    (o/subscribe source
      (zipmap
	callbacks
	(map #(o/observer nil % nil) callbacks))))
  (closed? [_]
    (o/closed? source))
  (cancel-callbacks [_ callbacks]
    (o/unsubscribe source callbacks)))

(defn constant-queue [source]
  (ConstantEventQueue. source))

