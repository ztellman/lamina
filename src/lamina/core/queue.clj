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
  (dequeue [this empty-value])
  (receive- [this callback])
  (listen [this callbacks])
  (on-close [this callbacks])
  (closed? [this])
  (cancel-callbacks [this callbacks]))

(defn receive [q callbacks]
  (let [msg (dequeue q ::none)]
    (if-not (= ::none msg)
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
   (dequeue [_ empty-value] empty-value)
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
  `(do
     (send-to-callbacks ~q
       (dosync
	 ~@body
	 (gather-callbacks (ensure (.q ~q)) ~q true)))
     true))

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
      (send-to-callbacks this
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
  (dequeue [_ empty-value]
    (dosync
      (if (empty? (ensure q))
	empty-value
	(dosync
	  (let [msg (first @q)]
	    (alter q pop)
	    msg)))))
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
	rc (ensure receivers)]
    (when-not (or (empty? rc) (empty? msgs))
      (let [msg (first msgs)]
	(ref-set receivers #{})
	[[msg rc]]))))

(defn gather-listeners [^EventQueue q msgs]
  (let [listeners (.listeners q)]
    (loop [msgs msgs, l (ensure listeners), result []]
      (if (empty? msgs)
	(do
	  (ref-set listeners l)
	  result)
	(let [msg (first msgs)
	      callbacks (->> l
			  (map #(when-let [[continue? f] (% msg)]
				  [f (when continue? %)]))
			  (remove nil?))]
	  (if (empty? callbacks)
	    (do
	      (ref-set listeners #{})
	      result)
	    (recur
	      (rest msgs)
	      (set (->> callbacks (map second) (remove nil?)))
	      (conj result [msg (map first callbacks)]))))))))

(defn gather-callbacks [msgs ^EventQueue q drop?]
  (let [l (gather-listeners q msgs)
	r (gather-receivers q msgs)
	drop-cnt (max (count l) (count r))]
    (when (and drop? (pos? drop-cnt))
      (ref-set (.q q)
	(loop [cnt drop-cnt, msgs msgs]
	  (if (zero? cnt)
	    msgs
	    (recur (dec cnt) (pop msgs))))))
    (when (pos? drop-cnt)
      (list*
	[(or (ffirst r) (ffirst l))
	 (concat (-> r first second) (-> l first second))]
	(rest l)))))

(defn send-to-callbacks [^EventQueue q msgs-and-targets]
  (when msgs-and-targets
    (doseq [[msg callbacks] msgs-and-targets]
      (doseq [c callbacks]
	(c msg)))
    (when (closed? q)
      (doseq [c @(.close-callbacks q)]
	(c))
      (dosync (ref-set (.close-callbacks q) nil)))))

(defn setup-observable->queue [accumulate ^EventQueue q]
  (let [src (source q)
	dst (distributor q)]
    (o/subscribe dst
      {q (o/observer
	   #(enqueue q %)
	   #(when (empty? @(.q q))
	      (enqueue q [nil]))
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
       (o/siphon source {distributor identity} true)
       (setup-observable->queue accumulate q)
       q)))

(defn copy-queue
  [^EventQueue q fs]
  (let [accumulate (atom true)
	source (source q)
	copies (map
		 (fn [f]
		   (let [distributor (o/observable)
			 copy (EventQueue.
				source
				distributor
				(ref nil)
				(ref #{})
				(ref #{})
				(ref #{})
				accumulate)]
		     (o/siphon source {distributor f} true)
		     copy))
		 fs)]
    (dosync
      (let [q (ensure (.q q))]
	(doseq [[f copy] (map list fs copies)]
	  (ref-set (.q copy) (f q))
	  (setup-observable->queue accumulate copy)))
      copies)))

;;;

(deftype ConstantEventQueue [^ConstantObservable source]
  EventQueueProto
  (source [_] source)
  (distributor [_] source)
  (enqueue [_ _] (assert false))
  (dequeue [_ empty-value]
    (let [val @(.val source)]
      (if (= o/empty-value val)
	empty-value
	val)))
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
		 (when-let [f* (second (dosync (f msg)))]
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

