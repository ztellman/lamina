;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.channel
  (:use
    [potemkin]
    [lamina.core.utils])
  (:require
    [lamina.core.graph :as g]
    [lamina.core.queue :as q]
    [lamina.core.lock :as l]
    [lamina.core.result :as r]
    [lamina.time :as t]
    [clojure.string :as str])
  (:import
    [lamina.core.utils
     IChannelMarker
     IEnqueue
     IError]
    [lamina.core.lock
     AsymmetricLock]
    [lamina.core.graph.node
     Node]
    [java.util.concurrent.atomic
     AtomicLong]
    [java.util.concurrent
     ConcurrentHashMap]
    [java.io
     Writer]))



;;;

(definterface+ IChannel
  (receiver-node [_]
    "Returns the receiver node for the channel.")
  (emitter-node [_]
    "Returns the emitter node for the channel.")
  (split-receiver [_]
    "Ensures the receiver and emitter are split, and returns the emitter."))

(deftype+ Channel
  [^Node receiver
   ^{:volatile-mutable true :tag Node} emitter
   metadata]

  IError
  (error [_ err force?]
    (error receiver err force?))

  clojure.lang.Counted
  (count [_]
    (count emitter))

  IEnqueue
  (enqueue [_ msg]
    (g/propagate receiver msg true))

  IChannelMarker
  IChannel
  (receiver-node [_]
    receiver)
  (emitter-node [this]
    emitter)
  (split-receiver [this]
    (if-let [n (g/split receiver)]
      (do
        (set! emitter n)
        true)
      false))

  clojure.lang.IMeta
  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ meta] (Channel. receiver emitter meta))

  Object
  (toString [_]
    (if-not (= ::none (g/error-value receiver ::none))
      (str "<== | ERROR: " (g/error-value receiver nil) " |")
      (if-let [q (g/queue emitter false)]
        (str "<== "
          (let [msgs (when q (q/messages q))
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

(deftype+ SplicedChannel [^Channel receiver ^Channel emitter metadata]
  clojure.lang.Counted
  (count [_]
    (count emitter))

  IEnqueue
  (enqueue [_ msg]
    (g/propagate (receiver-node receiver) msg true))

  IError
  (error [_ err force?]
    (error (receiver-node receiver) err force?))

  IChannelMarker
  IChannel
  (receiver-node [_]
    (receiver-node receiver))
  (emitter-node [_]
    (emitter-node emitter))
  (split-receiver [_]
    (split-receiver receiver))

  clojure.lang.IMeta
  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ meta] (SplicedChannel. receiver emitter meta))

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
  (let [n (g/mimic (emitter-node channel))]
    (Channel. n n nil)))

(defn closed-channel
  "Returns a closed channel containing the given messages."
  [& messages]
  (let [ch (channel* :messages (seq messages))]
    (g/close (receiver-node ch) false)
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
  "Returns a channel where all messages are enqueud into `receiver`, and
   consumed from `emitter`."
  [emitter receiver]
  (SplicedChannel.
    (if (instance? SplicedChannel receiver)
      (.receiver ^SplicedChannel receiver)
      receiver)
    (if (instance? SplicedChannel emitter)
      (.emitter ^SplicedChannel emitter)
      emitter)
    nil))

(defn channel?
  "Returns true if `x` is a channel.  This does not encompass result-channels."
  [x]
  (instance? IChannelMarker x))

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
   result-channel can represent any given message; calling `(read-channel ...)` multiple times
   will always consume multiple messages.

   Enqueueing a value into the result-channel before it is realized will prevent the message
   from being consumed, effectively cancelling the `read-channel` call."
  ([channel]
     (g/read-node (emitter-node channel))))

(defmacro read-channel*
  "A variant of `read-channel` with more options.

  `:timeout` - the timeout, in milliseconds.  If this elapses, the next message will not be consumed.

  `:predicate` - a function that takes the message, and returns true if it should be consumed.  If the
                 predicate returns false, the returned result will realize as value defined by `:on-false`.

  `:result` - the result that the read message should be enqueued into.  If the same result is used for 
              `read-channel` calls from multiple channels, this will have the effect of being realized
              as the first message from any of the channels, and not consuming any messages from the other
              channels.

   `:listener-result` - the result that will be returned to the emitter of the message, representing the
                        outcome of the consumption.  This should only be done if there is a clear single
                        outcome for this message (i.e. we're not just accumulating the entire stream.)

   `:on-timeout` - the result that will be realized if we timed out.  If not specified, the result will be
                   realized as a `:lamina/timeout` error.

    `:on-error` - the result that will be realized if the channel is in an error state.  If not specified,
                  the result will be realized as the channel's error.

    `:on-false` - the result that will be realized if the `:predicate` returns false.  Defaults to `:lamina/false`."
  [ch &
   {:keys [timeout
           predicate
           result
           listener-result
           on-timeout
           on-error
           on-false
           task-queue]
    :as options}]
  `(g/read-node* (emitter-node ~ch) ~@(apply concat options)))

(defn receive-all
  "Registers a `callback` that will consume all messages currently in the queue, and all
   subsequent messages that are enqueued into `channel`.

   This can be cancelled using `cancel-callback`."
  [channel callback]
  (g/link (emitter-node channel)
    callback
    (g/edge "receive-all" (g/callback-propagator callback))
    nil
    nil))

(defn cancel-callback
  "Cancels a callback registered with `receive`, `receive-all`, `on-closed`, `on-drained`, or `on-error`."
  ([channel callback]
     (g/cancel (emitter-node channel) callback)))

(defn sink
  "Creates a channel which will forward all messages to `callback`."
  [callback]
  (let [ch (channel)]
    (receive-all ch callback)
    ch))

(defn close
  "Closes the `channel`. Returns if successful, false if `channel` is permanent, already closed,
   or in an error state."
  [channel]
  (g/close (receiver-node channel) false))

(defn force-close
  "Closes `channel`, even if it is permanent. Returns if successful, false if `channel` is
   already closed or in an error state."
  [channel]
  (g/close (receiver-node channel) true))

(defn closed?
  "Returns true if `channel` is closed, false otherwise. "
  [channel]
  (g/closed? (receiver-node channel)))

(defn drained?
  "Returns true if `channel` is drained, false otherwise."
  [channel]
  (g/drained? (emitter-node channel)))

(defn transactional?
  "Returns true if `channel` has a transactional queue, false otherwise."
  [channel]
  (g/transactional? (receiver-node channel)))

(defn on-closed
  "Registers a callback that will be invoked with no arguments when `channel` is closed, or
   immediately if it has already been closed.  `callback` will only be invoked once, and can
   be cancelled using `cancel-callback`."
  [channel callback]
  (g/on-closed (receiver-node channel) callback))

(defn on-drained
  "Registers a callback that will be invoked with no arguments when `channel` is drained, or
   immediately if it has already been drained.  `callback` will only be invoked once, and can
   be cancelled using `cancel-callback`."
  [channel callback]
  (g/on-drained (emitter-node channel) callback))

(defn on-error
  "Registers a callback that will be called with the error when `channel` enters an error state,
   or immediately if it's already in an error state.  `callback` will only be invoked once,
   and can be cancelled using `cancel-callback`."
  [channel callback]
  (g/on-error (emitter-node channel) callback))

(defn error-value
  "Returns the error value of `channel`, returning 'default-value' if it's not an error state."
  [channel default-value]
  (g/error-value (emitter-node channel) default-value))

(defn closed-result
  "Returns a result-channel that will emit a result when `channel` is closed, or emit an error
   if `channel` goes into an error state."
  [channel]
  (g/closed-result (receiver-node channel)))

(defn drained-result
  "Returns a result-channel that will emit a result when `channel` is drained, or emit an error
   if `channel` goes into an error state."
  [channel]
  (g/drained-result (emitter-node channel)))

;;;

(defn- split [ch description sneaky?]
  (let [n (g/node identity)
        populate #(when-let [q (-> ch emitter-node (g/queue false))]
                    (q/append (g/queue n true) (q/messages q)))]
    (if (split-receiver ch)
      (let [emitter (split-receiver ch)]
        (g/join
          (receiver-node ch)
          (g/edge description n sneaky?)
          #(when % (populate))
          nil))
      (do
        (populate)
        (when (closed? ch)
          (g/close n false))
        (let [error (g/error-value (receiver-node ch) ::none)]
          (when-not (= ::none error)
            (error n error false)))))
    (Channel. n n nil)))

(defn fork
  "Returns a channel which is an exact duplicate of the source channel, containing all messages
   in the source channel's queue, and emitting all messages emitted by the source channel.

   If the forked channel is closed, the source channel is unaffected.  However, if the source
   channel is closed all forked channels are closed.  Similar propagation rules apply to error
   states."
  [channel]
  (split channel "fork" false))

(defn tap
  "Behaves like `fork`, except that the source channel will not remain open if only the tap
   exists downstream.

   If the tap channel is closed, the source channel is unaffected.  However, if the source
   channel is closed all tap channels are closed.  Similar propagation rules apply to error
   states."
  [channel]
  (split channel "tap" true))

(defn connect
  "A generalization of `siphon` and `join`, making sure that all messages in `src` will be forwarded
   to `dst`.

   If `upstream?` is true, when `dst` is closed, `src` will be closed.  This is true for `siphon` and `join`.

   If `downstream?` is true, when `src` is closed, `dst` will be closed.  This is true for `join`.

   The same behavior is also used for propagating error states."
  [src dst upstream? downstream?]
  (g/connect (emitter-node src) (receiver-node dst) upstream? downstream?)
  dst)

(defn siphon [src dst]
  (g/siphon (emitter-node src) (receiver-node dst))
  dst)

(defn join [src dst]
  (g/join (emitter-node src) (receiver-node dst))
  dst)

(defn bridge
  "A generalization of `bridge-join` and `bridge-siphon`.  Takes one `src` channel, and one or
   more downstream `dsts` channels.  All messages from `src` will be passed into `callback`, which 
   may or may not forward it to the downstream channels.

   This represents a relationship between channels which may or may not always result in messages
   propagating downstream.  This can be useful when certain channels are only used for specific
   types of messages, or there is an accumulation of messages, or anything else that is more complex
   than receive -> emit."
  [src dsts callback
   {:keys [description upstream? downstream?]
    :or {upstream? true, downstream? true}
    :as options}]
  (g/bridge
    (emitter-node src)
    (if (empty? dsts)
      [(g/terminal-propagator nil)]
      (map receiver-node dsts))
    callback
    (assoc options :node-description description)))

(defn bridge-join
  "A `bridge` between one `src` and one `dst` channel which is bidirectional."
  [src dst description callback]
  (bridge src [dst] callback {:description description}))

(defn bridge-siphon
  "A `bridge` between one `src` and one `dst` channel only propagates closing upstream."
  [src dst description callback]
  (bridge src [dst] callback {:description description, :downstream? false}))

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

;;;

(defmethod print-method Channel [o ^Writer w]
  (.write w (str o)))

(defmethod print-method SplicedChannel [o ^Writer w]
  (.write w (str o)))
