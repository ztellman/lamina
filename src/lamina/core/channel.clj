;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.channel
  (:use
    [lamina.core.timer])
  (:require
    [lamina.core.observable :as o]
    [lamina.core.queue :as q])
  (:import
    [lamina.core.observable ConstantObservable Observable]
    [lamina.core.queue ConstantEventQueue EventQueue]))

(defprotocol ChannelProtocol
  (consumer [_] "Observable that receives all messages enqueued into channel.")
  (queue [_] "The queue paired to the emitting observable."))

(deftype Channel
  [^Observable consumer ^EventQueue queue]
  ChannelProtocol
  (consumer [_] consumer)
  (queue [_] queue)
  (toString [_] (str queue)))

(defn channel [& messages]
  (let [source (o/observable)]
    (Channel. source (q/queue source messages))))

(defn channel? [ch]
  (satisfies? ChannelProtocol ch))

(deftype ConstantChannel
  [^ConstantObservable consumer ^ConstantEventQueue queue]
  ChannelProtocol
  (consumer [_] consumer)
  (queue [_] queue)
  (toString [_] (str (q/source queue))))

(defn constant-channel
  ([]
     (constant-channel o/empty-value))
  ([message]
     (let [source (o/constant-observable message)]
       (ConstantChannel. source (q/constant-queue source)))))

(defn constant-channel? [ch]
  (instance? ConstantChannel ch))

(def nil-channel
  (Channel. o/nil-observable q/nil-queue))

;;;

(defn sealed?
  "Returns true if no more messages can be enqueued into the channel."
  [ch]
  (-> ch consumer o/closed?))

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

(defn cancel-callback
  "Cancels one or more callbacks."
  [ch & callbacks]
  (-> ch queue (q/cancel-callbacks callbacks))
  (-> ch queue q/distributor (o/unsubscribe callbacks)))

(defn enqueue
  "Enqueues messages into the channel."
  [ch & messages]
  (-> ch consumer (o/message messages)))

(defn on-closed
  "Registers callbacks that will be triggered by the channel closing."
  [ch & callbacks]
  (-> ch queue (q/on-close callbacks)))

(defn on-sealed
  "Registers callbacks that will be triggered by the channel being sealed."
  [ch & callbacks]
  (-> ch consumer (o/subscribe (zipmap callbacks (map #(o/observer nil % nil) callbacks)))))

(defn close
  "Closes the channel."
  [ch]
  (-> ch consumer o/close))

(defn enqueue-and-close
  "Enqueues the final messages into the channel, sealing it.  When this message is
   received, the channel will be closed."
  [ch & messages]ch
  (apply enqueue ch messages)
  (when-not (constant-channel? ch)
    (close ch)))

(defn sealed-channel
  "Creates a channel which is already sealed."
  [& messages]
  (let [ch (channel)]
    (apply enqueue-and-close ch messages)
    ch))

(defn timed-channel [delay]
  (let [ch (constant-channel)]
    (delay-invoke #(enqueue ch nil) delay)
    ch))

(defn dequeue [ch empty-value]
  (-> ch queue (q/dequeue empty-value)))

;;;

(defn dequeue-from-channels [channel-map]
  (loop [s channel-map]
    (when-not (empty? s)
      (let [[k ch] (first s)]
	(let [val (dequeue ch ::none)]
	  (if (= ::none val)
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
     (if-let [val (dequeue-from-channels channel-map)]
       (constant-channel val)
       (if (zero? timeout)
	 (constant-channel nil)
	 (let [latch (ref false)
	       result (constant-channel)
	       callback (fn [key]
			  (fn [msg]
			    (when-not (ensure latch)
			      (ref-set latch true)
			      [false #(enqueue result [key %])])))]
	   (doseq [[k ch] channel-map]
	     (listen ch (callback k)))
	   (when (pos? timeout)
	     (delay-invoke
	       (fn []
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
