;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.channel
  (:use
    [lamina.core.timer])
  (:require
    [lamina.core.observable :as o]
    [lamina.core.queue :as q])
  (:import
    [lamina.core.observable ConstantObservable Observable]
    [lamina.core.queue ConstantEventQueue EventQueue]))

(defprotocol ChannelProtocol
  (consumer [_] "Observable that receives messages enqueued into the channel.")
  (emitter [_] "Observable that is the source of all messages from the channel.")
  (queue [_] "The queue paired to the emitter."))

(deftype Channel
  [^Observable consumer ^Observable emitter ^EventQueue queue]
  ChannelProtocol
  (consumer [_] consumer)
  (emitter [_] emitter)
  (queue [_] queue)
  (toString [_] (str queue)))

(defn channel [& messages]
  (let [source (o/observable)]
    (Channel. source source (q/queue source messages))))

(defn channel? [ch]
  (satisfies? ChannelProtocol ch))

(deftype ConstantChannel
  [^ConstantObservable consumer ^ConstantObservable emitter ^ConstantEventQueue queue]
  ChannelProtocol
  (consumer [_] consumer)
  (emitter [_] emitter)
  (queue [_] queue)
  (toString [_] (str emitter)))

(defn constant-channel
  ([]
     (constant-channel o/empty-value))
  ([message]
     (let [source (o/constant-observable message)]
       (ConstantChannel. source source (q/constant-queue source)))))

(defn constant-channel? [ch]
  (instance? ConstantChannel ch))

(def nil-channel
  (Channel. o/nil-observable o/nil-observable q/nil-queue))

;;;

(defn sealed?
  "Returns true if no more messages can be enqueued into the channel."
  [ch]
  (-> ch emitter o/closed?))

(defn closed?
  "Returns true if no more messages can be received from the channel."
  [ch]
  (-> ch queue q/closed?))

(defn listen 
  "Adds one or more callback which will receive all new messages.  If a callback returns a
   function, that function will consume the message.  Otherwise, it should return nil.  The
   callback is run within a transaction, and may receive the same message multiple times.

   This exists to support poll, don't use it directly unless you know what you're doing."
  [ch & callbacks]
  (-> ch queue (q/listen callbacks)))

(defn receive
  "Adds one or more callbacks which will receive the next message from the channel."
  [ch & callbacks]
  (-> ch queue (q/receive callbacks)))

;; bridging atoms and refs ain't easy
(defn receive-all
  "Adds one or more callbacks which will receive all messages from the channel."
  [ch & callbacks]
  (if (closed? ch)
    false
    (let [observers (zipmap
		      callbacks
		     (map
		       (fn [f] (o/observer #(doseq [m %] (f m))))
		       callbacks))
	  distributor (-> ch queue q/distributor)]
      (if (o/constant-observable? distributor)
	(do
	  (o/subscribe distributor observers)
	  true)
	(let [q (-> ch ^EventQueue queue .q)
	      spillover (ref [])]
	  
	  ;;first, create a temporary observer that either diverts messages
	  ;;into the queue or into the callbacks
	  (o/subscribe distributor
	    {(first callbacks)
	     (o/observer
	       (fn [msgs]
		 (let [msgs (dosync
			      (ensure q)
			      (ensure spillover)
			      (let [s @spillover]
				(when (= ::done s)
				  msgs)))]
		   (doseq [m msgs]
		     (doseq [c callbacks]
		       (c m))))))})
	  
	  ;;then, empty out the queue into the callbacks
	  (let [msgs (dosync
		       (ensure q)
		       (ensure spillover)
		       (let [msgs @q]
			 (ref-set q clojure.lang.PersistentQueue/EMPTY)
			 (ref-set spillover ::done)
			 msgs))]
	    (doseq [m msgs]
	      (doseq [c callbacks]
		(c m)))
	    
	    ;;finally, replace the temporary observer with the permanent versions
	    (o/subscribe distributor
	      (zipmap
		callbacks
		(map
		  #(o/observer
		     (fn [msgs]
		       (doseq [m msgs]
			 (% m))))
		  callbacks)))

	    true))))))

(defn cancel-callback
  "Cancels one or more callbacks."
  [ch & callbacks]
  (-> ch queue (q/cancel-callbacks callbacks))
  (-> ch queue q/distributor (o/unsubscribe callbacks)))

(defn enqueue
  "Enqueues messages into the channel."
  [ch & messages]
  (-> ch consumer (o/message messages)))

(defn on-close
  "Registers callbacks that will be triggered by the channel closing."
  [ch & callbacks]
  (-> ch queue (q/on-close callbacks)))

(defn on-sealed
  "Registers callbacks that will be triggered by the channel being sealed."
  [ch & callbacks]
  (-> ch emitter (o/subscribe (zipmap callbacks (map #(o/observer nil % nil) callbacks)))))

(defn enqueue-and-close
  "Enqueues the final messages into the channel, sealing it.  When this message is
   received, the channel will be closed."
  [ch & messages]
  (-> ch emitter (o/message messages)))

(defn close
  "Closes the channel."
  [ch]
  (enqueue-and-close ch nil))

(defn sealed-channel
  "Creates a channel which is already sealed."
  [& messages]
  (let [ch (channel)]
    (apply enqueue-and-close ch messages)
    ch))

;;;

(defn pop-from-channels [channel-map]
  (loop [s channel-map]
    (when-not (empty? s)
      (let [[k ch] (first s)]
	(let [val (-> ch queue q/pop-)]
	  (if (= o/empty-value val)
	    (recur (rest s))
	    [k val]))))))

(defn poll
   "Allows you to consume exactly one message from multiple channels.

   If the function is called with (poll {:a a, :b b}), and channel 'a' is
   the first to emit a message, the function will return a constant channel
   which emits [:a message].

   If the poll times out, the constant channel will emit 'nil'.  If a timeout
   is not specified, the poll will never time out."
  ([channel-map]
     (poll channel-map -1))
  ([channel-map timeout]
     (if-let [val (pop-from-channels channel-map)]
       (constant-channel val)
       (if (zero? timeout)
	 (constant-channel nil)
	 (let [latch (ref false)
	       result (constant-channel)
	       callback (fn [key]
			  (fn [msg]
			    (ensure latch)
			    (when-not @latch
			      (ref-set latch true)
			      [false #(enqueue result [key %])])))]
	  (doseq [[k ch] channel-map]
	    (listen ch (callback k)))
	  (when (pos? timeout)
	    (delay-invoke
	      (fn []
		(println "timeout")
		(dosync (ref-set latch true))
		(enqueue result nil))
	      timeout))
	  result)))))

;;;

(defn splice
  "Splices together a message source and a message destination
   into a single channel."
  [src dst]
  (Channel.
    (consumer dst)
    (emitter src)
    (queue src)))

(defn channel-pair
  "Creates paired channels, where an enqueued message from one channel
   can be received from the other."
  ([]
     (channel-pair (channel) (channel)))
  ([a b]
     [(splice a b) (splice b a)]))

;;;

(defmethod print-method Channel [ch writer]
  (.write writer (str "<== " (str ch))))

(defmethod print-method ConstantChannel [ch writer]
  (let [s (str ch)]
    (.write writer (str "<== [" s (when-not (empty? s) " ...") "]"))))
