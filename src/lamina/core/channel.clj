;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.channel
  (:use
    [lamina.core.utils])
  (:require
    [lamina.core.graph :as g]
    [lamina.core.queue :as q]
    [lamina.core.lock :as l]
    [lamina.core.result :as r]
    [lamina.core.threads :as t]
    [clojure.string :as str])
  (:import
    [lamina.core.lock
     AsymmetricLock]
    [lamina.core.graph.node
     Node]
    [java.util.concurrent.atomic
     AtomicLong]
    [java.io
     Writer]))

(set! *warn-on-reflection* true)

;;;

(defprotocol IChannel
  (receiver-node [_]
    "Returns the receiver node for the channel.")
  (emitter-node [_]
    "Returns the emitter node for the channel.")
  (split-receiver [_]
    "Ensures the receiver and emitter are split, and returns the emitter."))

(deftype Channel
  [^Node receiver
   ^{:volatile-mutable true :tag Node} emitter
   metadata]

  IEnqueue
  (enqueue [_ msg]
    (g/propagate receiver msg true))

  IChannel
  (receiver-node [_]
    receiver)
  (emitter-node [this]
    emitter)
  (split-receiver [this]
    (if-let [n (g/split receiver)]
      (do
        (set! emitter n)
        n)
      emitter))

  clojure.lang.IMeta
  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ meta] (Channel. receiver emitter meta))

  Object
  (toString [_]
    (if-not (= ::none (g/error-value receiver ::none))
      (str "<== | ERROR: " (g/error-value receiver nil) " |")
      (if-let [q (g/queue emitter)]
        (str "<== "
          (let [msgs (q/messages q)
                msgs-str (-> msgs vec str)]
            (str
              (first msgs-str)
              (subs msgs-str 1 (dec (count msgs-str)))
              (when-not (g/closed? receiver)
                (str
                  (when-not (empty? msgs) " ")
                 "\u2026"))
              (last msgs-str))))
        (str "<== "
          (if-not (g/closed? receiver)
            "[ \u2026 ]"
            "[ ]"))))))

(defrecord SplicedChannel [^Channel receiver ^Channel emitter]
  IEnqueue
  (enqueue [_ msg]
    (g/propagate (receiver-node receiver) msg true))
  IChannel
  (receiver-node [_]
    (receiver-node receiver))
  (emitter-node [_]
    (emitter-node emitter))
  (split-receiver [_]
    (split-receiver receiver))
  Object
  (toString [_]
    (str emitter)))

(defmacro channel*
  "A general-purpose channel creator.  Can be used to mix and match various properties, such as

     (channel* :grounded? true, :description \"my very own grounded channel\")

     :grounded?        - ensures that messages cannot accumulate in the queue
     :permanent?       - ensures that the channel cannot be closed or be put in an error state
     :transactional?   - determines whether the channel's queue is transactional
     :messages         - sequence of zero or more messages that will be in the channel's queue
     :description      - a string that will be diplayed in channel visualizations
     :meta             - initial metadata"
  [& {:keys [grounded? permanent? transactional? messages description meta] :as options}]
  `(let [n# (g/node* ~@(apply concat options))]
     (Channel. n# n# ~meta)))

(defn channel
  "Returns a channel containing the given messages."
  [& messages]
  (channel* :messages (seq messages)))

(defn mimic [channel]
  (let [n (g/mimic (receiver-node channel))]
    (Channel. n n nil)))

(defn closed-channel
  "Returns a closed channel containing the given messages."
  [& messages]
  (let [ch (channel* :messages (seq messages))]
    (g/close (receiver-node ch))
    ch))

(defn grounded-channel
  "Returns a channel that cannot accumulate messages."
  []
  (channel* :grounded? true))

(defn ground
  "Ensures that messages will not accumulate in the channel's queue."
  [ch]
  (g/ground (emitter-node ch)))

(defn splice
  "Returns a channel where all messages are enqueud into 'receiver', and
   consumed from 'emitter'."
  [emitter receiver]
  (SplicedChannel.
    (if (instance? SplicedChannel receiver)
      (.receiver ^SplicedChannel receiver)
      receiver)
    (if (instance? SplicedChannel emitter)
      (.emitter ^SplicedChannel emitter)
      emitter)))

(defn channel?
  "Returns true if 'x' is a channel.  This does not encompass result-channels."
  [x]
  (or
    (instance? Channel x)
    (instance? SplicedChannel x)))

;;;

(defn receive
  "Registers a callback that will be invoked with the next message enqueued into the channel, or
   the first message already in the queue.  Only one callback can consume any given message;
   registering multiple callbacks will consume multiple messages.

   This can be cancelled using cancel-callback."
  [channel callback]
  (g/receive (emitter-node channel) callback callback))

(defn read-channel
  "Returns a result-channel representing the next message from the channel.  Only one
   result-channel can represent any given message; calling (read-channel ...) multiple times
   will always consume multiple messages.

   Enqueueing a value into the result-channel before it is realized will prevent the message
   from being consumed, effectively cancelling the read-channel call."
  ([channel]
     (g/read-node (emitter-node channel))))

(defmacro read-channel*
  "something goes here"
  [ch & {:keys [timeout predicate result on-timeout on-error on-false] :as options}]
  `(g/read-node* (emitter-node ~ch) ~@(apply concat options)))

(defn receive-all
  "Registers a callback that will consume all messages currently in the queue, and all
   subsequent messages that are enqueued into the channel.

   This can be cancelled using cancel-callback."
  [channel callback]
  (g/link (emitter-node channel)
    callback
    (g/edge "receive-all" (g/callback-propagator callback))
    nil
    nil))

(defn sink
  "Equivalent to receive-all, but with argument ordering more amenable to threading with
   the ->> operator."
  [callback channel]
  (receive-all channel callback))

(defn cancel-callback
  "Cancels a callback registered with receive, receive-all, on-closed, on-drained, or on-error."
  ([channel callback]
     (g/cancel (emitter-node channel) callback)))

(defn fork
  "Returns a channel which is an exact duplicate of the source channel, containing all messages
   in the source channel's queue, and emitting all messages emitted by the source channel.

   If the forked channel is closed, the source channel is unaffected.  However, if the source
   channel is closed all forked channels are closed.  Similar propagation rules apply to error
   states."
  [channel]
  (let [n (g/node identity)
        emitter (split-receiver channel)]
    (g/join
      (receiver-node channel)
      (g/edge "fork" n)
      #(when %
         (when-let [q (g/queue emitter)]
           (-> n g/queue (q/append (q/messages q)))))
      nil)
    (Channel. n n nil)))

(defn close
  "Closes the channel. Returns if successful, false if the channel is already closed or in an
   error state."
  [channel]
  (g/close (receiver-node channel)))

(defn error [channel err]
  (g/error (receiver-node channel) err))

(defn closed?
  "Returns true if the channel is closed, false otherwise. "
  [channel]
  (g/closed? (receiver-node channel)))

(defn drained?
  "Returns true if the channel is drained, false otherwise."
  [channel]
  (g/drained? (emitter-node channel)))

(defn transactional?
  "Returns true if the channel's queue is transactional, false otherwise."
  [channel]
  (g/transactional? (receiver-node channel)))

(defn on-closed
  "Registers a callback that will be invoked with no arguments when the channel is closed, or
   immediately if it has already been closed.  This callback will only be invoked once, and can
   be cancelled using cancel-callback."
  [channel callback]
  (g/on-closed (receiver-node channel) callback))

(defn on-drained
  "Registers a callback that will be invoked with no arguments when the channel is drained, or
   immediately if it has already been drained.  This callback will only be invoked once, and can
   be cancelled using cancel-callback."
  [channel callback]
  (g/on-drained (emitter-node channel) callback))

(defn on-error
  "Registers a callback that will be called with the error when the channel enters an error state,
   or immediately if it's already in an error state.  This callback will only be invoked once,
   and can be cancelled using cancel-callback."
  [channel callback]
  (g/on-error (emitter-node channel) callback))

(defn closed-result
  "Returns a result-channel that will emit a result when the channel is closed, or emit an error
   if the channel goes into an error state."
  [channel]
  (g/closed-result (receiver-node channel)))

(defn drained-result
  "Returns a result-channel that will emit a result when the channel is drained, or emit an error
   if the channel goes into an error state."
  [channel]
  (g/drained-result (emitter-node channel)))

;;;

(defn siphon [src dst]
  (g/siphon (emitter-node src) (receiver-node dst)))

(defn join [src dst]
  (g/join (emitter-node src) (receiver-node dst)))

(defn bridge-join
  "something goes here"
  [src description callback & dsts]
  (g/bridge-join (emitter-node src) nil description callback
    (if (empty? dsts)
      [(g/terminal-propagator nil)]
      (map receiver-node dsts))))

(defn bridge-siphon
  "something goes here"
  [src description callback & dsts]
  (g/bridge-siphon (emitter-node src) nil description callback
    (if (empty? dsts)
      [(g/terminal-propagator nil)]
      (map receiver-node dsts))))

(defn map*
  "A dual to map.

   (map* inc (channel 1 2 3)) => [2 3 4]"
  [f channel]
  (let [n (g/downstream-node f (emitter-node channel))]
    (Channel. n n nil)))

(defn filter*
  "A dual to filter.

   (filter* odd? (channel 1 2 3)) => [1 3]"
  [f channel]
  (let [n (g/downstream-node (predicate-operator f) (emitter-node channel))]
    (Channel. n n nil)))

(defn remove*
  "A dual to remove.

   (remove* even? (channel 2 3 4)) => [3]"
  [f channel]
  (filter* (complement f) channel))

(defn multiplexer [facet generator]
  (let [receiver (g/node identity)
        propagator (g/multiplexing-propagator facet
                     (fn [id]
                       (let [ch (channel* :description (pr-str id))
                             ch* (generator id)]
                         (join ch ch*)
                         (receiver-node ch))))]
    (g/link receiver propagator (g/edge nil propagator) nil nil)
    (Channel. receiver receiver nil)))

;;;

(defn check-idle [^AtomicLong last-message interval result]
  (let [mark (.get last-message)]
    (t/delay-invoke (- interval (- (System/currentTimeMillis) mark))
      (fn []
        (if (= mark (.get last-message))
          (r/success result true)
          (check-idle last-message interval result))))))

(defn idle-result [interval ch]
  (let [last-message (AtomicLong. (System/currentTimeMillis))
        result (r/result-channel)]
    (receive-all ch (fn [_] (.set last-message (System/currentTimeMillis))))
    (check-idle last-message interval result)
    result))

(defn close-on-idle [interval ch]
  (r/subscribe (idle-result interval ch)
    (r/result-callback
      (fn [_] (close ch))
      (fn [_])))
  nil)

;;;

(defmethod print-method Channel [o ^Writer w]
  (.write w (str o)))

(defmethod print-method SplicedChannel [o ^Writer w]
  (.write w (str o)))