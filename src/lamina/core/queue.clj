;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.queue
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
  (on-drained [this callbacks])
  (drained? [this])
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
   (on-drained [_ _] false)
   (drained? [_] true)
   (cancel-callbacks [_ _])
   (toString [_] "[]")))

;;;

(declare gather-receivers)

(declare gather-listeners)

(declare gather-callbacks)

(declare send-to-callbacks)

(declare check-for-drained)

(defmacro update-and-send [q & body]
  `(do
     (send-to-callbacks ~q
       (dosync
	 ~@body
	 (gather-callbacks (ensure (.q ~q)) ~q true)))
     true))

(deftype EventQueue
  [source distributor q
   receivers listeners drained-callbacks
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
  (on-drained [this callbacks]
    (when (dosync
	    (ensure drained-callbacks)
	    (if (drained? this)
	      true
	      (do
		(apply alter drained-callbacks conj callbacks)
		false)))
      (doseq [c callbacks]
	(c))))
  (dequeue [this empty-value]
    (dosync
      (if (empty? (ensure q))
	empty-value
	(dosync
	  (let [msg (first (ensure q))]
	    (alter q pop)
	    (check-for-drained this)
	    msg)))))
  (receive- [this callbacks]
    (update-and-send this
      (apply alter receivers conj callbacks)))
  (listen [this callbacks]
    (update-and-send this
      (apply alter listeners conj callbacks)))
  (drained? [_]
    (and (o/closed? source) (empty? @q)))
  (cancel-callbacks [_ callbacks]
    (dosync
      (apply alter drained-callbacks disj callbacks)
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

(defn check-for-drained [^EventQueue q]
  (when (drained? q)
    (doseq [c @(.drained-callbacks q)]
      (c))
    (dosync (ref-set (.drained-callbacks q) nil))))

(defn send-to-callbacks [^EventQueue q msgs-and-targets]
  (when msgs-and-targets
    (doseq [[msg callbacks] msgs-and-targets]
      (doseq [c callbacks]
	(c msg))))
  (check-for-drained q))

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
     (queue source (o/observable) messages))
  ([source distributor messages]
     (let [accumulate (atom true)
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
       (o/siphon source {distributor identity} 0 true)
       (setup-observable->queue accumulate q)
       q)))

(defn copy-queue
  ([q]
     (copy-queue q (source q)))
  ([^EventQueue q src]
     (let [copy ^EventQueue (queue src)]
       (dosync (ref-set (.q copy) (ensure (.q q))))
       copy)))

(defn copy-and-alter-queue
  ([q f]
     (copy-and-alter-queue q (source q) f))
  ([^EventQueue q src f]
     (let [copy ^EventQueue (queue (o/observable))]
       (o/siphon src {(source copy) f} -1 true)
       (dosync
	 (let [q (ensure (.q q))]
	   (ref-set (.q copy)
	     (if-not (empty? q)
	       (apply conj clojure.lang.PersistentQueue/EMPTY (f q))
	       clojure.lang.PersistentQueue/EMPTY))))
       copy)))

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
  (on-drained [_ callbacks]
    )
  (drained? [_]
    false)
  (cancel-callbacks [_ callbacks]
    (o/unsubscribe source callbacks)))

(defn constant-queue [source]
  (ConstantEventQueue. source))

