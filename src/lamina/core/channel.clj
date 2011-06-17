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
    [lamina.core utils timer])
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
  [^Observable consumer ^EventQueue queue metadata]
  ChannelProtocol
  (consumer [_] consumer)
  (queue [_] queue)
  (toString [_] (str queue))
  clojure.lang.Counted
  (count [q] (count queue))
  clojure.lang.IMeta
  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ meta] (Channel. consumer queue meta)))

(defn channel [& messages]
  (let [source (o/observable)]
    (Channel. source (q/queue source messages) nil)))

(defn permanent-channel [& messages]
  (let [source (o/permanent-observable)]
    (Channel. source (q/queue source (o/permanent-observable) messages) nil)))

(deftype ConstantChannel
  [^ConstantObservable consumer ^ConstantEventQueue queue metadata]
  ChannelProtocol
  (consumer [_] consumer)
  (queue [_] queue)
  (toString [_] (str (q/source queue)))
  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ meta] (ConstantChannel. consumer queue meta)))

(defn constant-channel
  ([]
     (constant-channel o/empty-value))
  ([message]
     (let [source (o/constant-observable message)]
       (ConstantChannel. source (q/constant-queue source) nil))))

(defn constant-channel? [ch]
  (instance? ConstantChannel ch))

(defn channel? [ch]
  (or
    (instance? Channel ch)
    (instance? ConstantChannel ch)))

(defn proxy-channel [f ch]
  (if (constant-channel? ch)
    (ConstantChannel. (o/proxy-observable f (consumer ch)) (queue ch) nil)
    (Channel. (o/proxy-observable f (consumer ch)) (queue ch) nil)))

(def nil-channel
  (Channel. o/nil-observable q/nil-queue nil))

;;;

(defn closed?
  "Returns true if no more messages can be enqueued into the channel."
  [ch]
  (-> ch consumer o/closed?))

(defn drained?
  "Returns true if no more messages can be received from the channel."
  [ch]
  (-> ch queue q/drained?))

(defn listen 
  "Adds one or more callback which will receive all new messages.  If a callback returns a
   function, that function will consume the message.  Otherwise, it should return nil.  The
   callback is run within a transaction, and may receive the same message multiple times.


   This exists to support poll, don't use it directly unless you know what you're doing."
  ([ch a]
     (-> ch queue (q/listen (unwrap-fn a))))
  ([ch a b]
     (-> ch queue (q/listen (unwrap-fn a) (unwrap-fn b))))
  ([ch a b & rest]
     (apply q/listen (queue ch) (map unwrap-fn (list* a b rest)))))

(defn receive
  "Adds one or more callbacks which will receive the next message from the channel."
  ([ch a]
     (-> ch queue (q/receive (unwrap-fn a))))
  ([ch a b]
     (-> ch queue (q/receive (unwrap-fn a) (unwrap-fn b))))
  ([ch a b & callbacks]
     (apply q/receive (queue ch) (map unwrap-fn (list* a b rest)))))

(defn cancel-callback
  "Cancels one or more callbacks."
  [ch & callbacks]
  (let [callbacks (map unwrap-fn callbacks)]
    (-> ch queue (q/cancel-callbacks callbacks))
    (-> ch consumer (o/unsubscribe callbacks))
    (-> ch queue q/distributor (o/unsubscribe callbacks))
    nil))

(defn enqueue
  "Enqueues messages into the channel."
  [ch & messages]
  (-> ch consumer (o/message messages)))

(defn on-drained
  "Registers callbacks that will be triggered by the channel being drained."
  [ch & callbacks]
  (-> ch queue (q/on-drained (map unwrap-fn callbacks))))

(defn on-closed
  "Registers callbacks that will be triggered by the channel closing."
  [ch & callbacks]
  (let [callbacks (map unwrap-fn callbacks)]
    (-> ch consumer (o/subscribe (zipmap callbacks (map #(o/observer nil % nil) callbacks))))))

(defn close
  "Closes the channel."
  [ch]
  (-> ch consumer o/close))

(defn enqueue-and-close
  "Enqueues the final messages into the channel, sealing it.  When this message is
   received, the channel will be closed."
  [ch & messages]
  (let [result (apply enqueue ch messages)]
    (when-not (constant-channel? ch)
      (close ch))
    result))

(defn closed-channel
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
			      [true #(enqueue result [key %])])))]
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
  "Splices together a message emitter and a message receiver
   into a single channel."
  [emitter receiver]
  (Channel.
    (consumer receiver)
    (queue emitter)
    {}))

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
