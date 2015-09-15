;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core
  (:use
    [potemkin])
  (:require
    [manifold
     [stream :as s]
     [deferred :as d]]
    [manifold.stream.core :as manifold]
    [lamina.time :as t]
    [lamina.core.watch :as w]
    [lamina.core.named :as n]
    [lamina.core.utils :as u]
    [lamina.core.channel :as ch]
    [lamina.core.pipeline :as p]
    [lamina.core.result :as r]
    [lamina.core.operators :as op]))

;;;

(import-vars
  [lamina.core.channel

   channel
   closed-channel
   grounded-channel
   channel*
   splice
   channel?
   ground

   receive
   receive-all
   read-channel
   read-channel*
   sink
   fork
   tap

   close
   force-close
   closed?
   drained?
   transactional?
   on-closed
   on-drained
   on-error
   cancel-callback
   closed-result
   drained-result

   map*
   filter*
   remove*]

  [lamina.core.watch

   atom-sink
   watch-channel]

  [lamina.core.operators

   channel->lazy-seq
   channel->seq

   mapcat*
   concat*
   take*
   drop*
   drop-while*
   take-while*
   reductions*
   reduce*
   last*
   partition*
   partition-all*

   receive-in-order
   emit-in-order
   combine-latest
   merge-channels
   transitions
   zip
   zip-all

   periodically
   sample-every
   partition-every

   defer-onto-queue

   distributor
   distribute-aggregate]

  [lamina.core.named

   named-channel])

(defn enqueue
  "Enqueues the message or messages into the channel."
  ([channel]
     nil)
  ([channel message]
     (u/enqueue channel message))
  ([channel message & messages]
     (doall
       (map
         #(u/enqueue channel %)
         (list* message messages)))))

(defn enqueue-and-close
  "Enqueues the message or messages into the channel, and then closes the channel."
  [ch & messages]
  (apply enqueue ch messages)
  (close ch))

(defn permanent-channel
  "Returns a channel which cannot be closed or put into an error state."
  [& messages]
  (channel*
    :permanent? true
    :messages (seq messages)))

(defmacro channel-seq [& args]
  (println "channel-seq is deprecated, use channel->seq instead.")
  `(channel->seq ~@args))

(defmacro lazy-channel-seq [& args]
  (println "lazy-channel-seq is deprecated, use channel->lazy-seq instead.")
  `(channel->lazy-seq ~@args))

(defn lazy-seq->channel
  "Returns a channel representing the elements of the sequence."
  [s]
  (let [ch (channel)]

    (future
      (try
        (loop [s s]
          (when-not (empty? s)
            (enqueue ch (first s))
            (when-not (closed? ch)
              (recur (rest s)))))
        (catch Exception e
          (u/log-error e "Error in lazy-seq->channel."))
        (finally
          (close ch))))

    ch))

;;;

(import-vars
  [lamina.core.result

   result-channel
   async-promise?
   with-timeout
   expiring-result
   timed-result
   success
   success-result
   error-result
   merge-results])

(defn on-realized
  "Allows two callbacks to be registered on a result-channel, one in the case of a
   value, the other in case of an error.

   This often can and should be replaced by a pipeline."
  [result-channel on-success on-error]
  (r/subscribe result-channel
    (r/result-callback
      (or on-success (fn [_]))
      (or on-error (fn [_])))))

;;;

(defn error
  "Puts the channel or result-channel into an error state."
  [channel err]
  (u/error channel err false))

(defn force-error
  "Puts the channel or result-channel into an error state, even if it's permanent."
  [channel err]
  (u/error channel err true))

(defn siphon
  "Takes all messages from `src` and forwards them to `dst`.  If `dst` closes, `src` is closed, but
   not vise-versa.  Error states are similarly propagated.  This is useful for many transient channels
   feeding into one channel.

   If more than two channels are specified, `siphon` becomes transitive.  `(siphon a b c)` is equivalent to
   `(siphon a b)` and `(siphon b c)`."
  ([src dst]
     (if (async-promise? src)
       (r/siphon-result src dst)
       (ch/siphon src dst)))
  ([src dst & rest]
     (siphon src dst)
     (apply siphon dst rest)))

(defn join
  "Takes all messages from `src` and forwards them to `dst`.  If either channel closes or goes into an
   error state, the same is done for the other channel.  This is useful for channels which have a 1-to-1
   relationship.

   If more than two channels are specified, `join` becomes transitive.  `(join a b c)` is equivalent to
   `(join a b)` and `(join b c)`."
  ([src dst]
     (if (async-promise? src)
       (do
         (r/siphon-result src dst)
         (r/subscribe dst (r/result-callback (fn [_]) #(u/error src % false))))
       (ch/join src dst)))
  ([src dst & rest]
     (join src dst)
     (apply join dst rest)))

(defn error-value
  "Returns the error-value of the channel or async-promise if it's in an error state, and 'default-value'
   otherwise"
  [x default-value]
  (if (async-promise? x)
    (r/error-value x default-value)
    (ch/error-value x default-value)))

(defn channel-pair
  "Returns a pair of channels, where all messages enqueued into one channel can
   be received by the other, and vice-versa.  Closing one channel will automatically
   close the other."
  []
  (let [a (channel)
        b (channel)]
    (on-closed a #(close b))
    (on-closed b #(close a))
    (on-error a #(error b %))
    (on-error b #(error a %))
    [(splice a b) (splice b a)]))

(import-vars
  [lamina.core.pipeline

   pipeline
   run-pipeline
   restart
   redirect
   complete
   wait-stage])

(defmacro sink->>
  "Creates a channel, pipes it through the ->> operator, and sends the
   resulting stream into the callback. This can be useful when defining
   :probes for an instrumented function, among other places.

   (sink->> (map* inc) (map* dec) println)

   expands to

   (let [ch (channel)]
     (receive-all
       (->> ch (map* inc) (map* dec))
       println)
     ch)"
  [& transforms+callback]
  (let [transforms (butlast transforms+callback)
        callback (last transforms+callback)]
   (unify-gensyms
     `(let [ch## (channel)]
        (do
          ~(if (empty? transforms)
             `(receive-all ch## ~callback)
             `(receive-all (->> ch## ~@transforms) ~callback))
          ch##)))))

(defmacro split
  "Returns a channel which will forward each message to all downstream-channels.
   This can be used with sink->>, siphon->>, and join->> to define complex
   message flows:

   (join->> (map* inc)
     (split
       (sink->> (filter* even?) log-even)
       (sink->> (filter* odd?) log-odd)))"
  [& downstream-channels]
  `(let [ch# (channel)]
     (doseq [x# (list ~@downstream-channels)]
       (siphon ch# x#))
     ch#))

(defmacro siphon->>
  "A variant of sink->> where the last argument is assumed to be a channel,
   and the contents of the transform chain are siphoned into it.

   (siphon->> (map* inc) (map* dec) (named-channel :foo))

   expands to

   (let [ch (channel)]
     (siphon
       (->> ch (map* inc) (map* dec))
       (named-channel :foo))
     ch)"
  [& transforms+downstream-channel]
  (let [transforms (butlast transforms+downstream-channel)
        ch (last transforms+downstream-channel)]
    (unify-gensyms
      `(let [ch## (channel)]
         (siphon
           ~(if (empty? transforms)
              `ch##
              `(->> ch## ~@transforms))
           ~ch)
         ch##))))

(defmacro join->>
  "A variant of sink->> where the last argument is assumed to be a channel,
   and the transform chain is joined to it.

   (join->> (map* inc) (map* dec) (named-channel :foo))

   expands to

   (let [ch (channel)]
     (join
       (->> ch (map* inc) (map* dec))
       (named-channel :foo))
     ch)"
  [& transforms+downstream-channel]
  (let [transforms (butlast transforms+downstream-channel)
        ch (last transforms+downstream-channel)]
    (unify-gensyms
      `(let [ch## (channel)]
         (join
           ~(if (empty? transforms)
              `ch##
              `(->> ch## ~@transforms))
           ~ch)
         ch##))))

;;;

(defn idle-result
  "A result which will be realized if `channel` doesn't emit a message for `interval` milliseconds."
  ([interval channel]
     (idle-result interval (t/task-queue) channel))
  ([interval task-queue channel]
     (let [target-time (atom (+ (t/now task-queue) interval))
           ch (tap channel)]

       (ground ch)

       (p/run-pipeline ch
         {:error-handler (fn [_])}
         #(read-channel* %
            :task-queue task-queue
            :timeout (- @target-time (t/now task-queue))
            :priority Integer/MAX_VALUE
            :on-timeout ::timeout
            :on-drained ::timeout)
         #(if (= ::timeout %)
            (p/complete true)
            (let [to-sleep (- @target-time (t/now task-queue))]
              (r/timed-result to-sleep to-sleep task-queue)))
         (fn [slept]
           (swap! target-time + (- interval slept))
           (restart))))))

(defn close-on-idle
  "Sets up a watcher which will close `channel` if it doesn't emit a message for `interval` milliseconds.

   Returns `channel`, for chaining convenience."
  ([interval channel]
     (close-on-idle interval (t/task-queue) channel))
  ([interval task-queue channel]
     (p/run-pipeline (idle-result interval task-queue channel)
       {:error-handler (fn [_])}
       (fn [_] (close channel)))
     channel))

(defn wait-for-message
  "Blocks for the next message from the channel. If the timeout elapses without a message,
   throws a java.util.concurrent.TimeoutException."
  ([ch]
     @(read-channel ch))
  ([ch timeout]
     @(read-channel* ch :timeout timeout)))

(defn wait-for-result
  "Waits for the result to be realized. If the timeout elapses without a value, throws a
   java.util.concurrent.TimeoutException."
  ([result-channel]
     @result-channel)
  ([result-channel timeout]
     @(with-timeout timeout result-channel)))

;;;

(manifold/def-source LaminaChannelSource
  [ch]

  (isSynchronous [_] false)

  (description [this]
    {:source? true
     :drained? (drained? ch)
     :type "lamina"})

  (close [_]
    (close ch))

  (isDrained [this]
    (drained? ch))

  (onDrained [this callback]
    (on-drained this callback))

  (take [this default-val blocking?]
    (let [d (d/->deferred
              (p/run-pipeline
                (read-channel* ch
                  :on-error default-val)
                {:error-handler (fn [x] (p/complete default-val))}
                (fn [x]
                  (if (identical? :lamina/drained! x)
                    default-val
                    x))))]
      (if blocking?
        @d
        d)))

  (take [this default-val blocking? timeout timeout-val]
    (let [d (d/->deferred
              (p/run-pipeline
                (read-channel* ch
                  :timeout timeout
                  :on-timeout timeout-val
                  :on-error default-val)
                {:error-handler (fn [x] (p/complete default-val))}
                (fn [x]
                  (if (identical? :lamina/drained! x)
                    default-val
                    x))))]
      (if blocking?
        @d
        d))))

(manifold/def-sink LaminaChannelSink
  [ch]

  (isSynchronous [_] false)

  (description [this]
    {:sink? true
     :closed? (closed? ch)
     :type "lamina"})

  (close [this]
    (close ch))

  (isClosed [this]
    (closed? ch))

  (onClosed [this callback]
    (on-closed ch callback))

  (put [this x blocking?]
    (let [x (enqueue ch x)
          x (cond
              (r/async-promise? x)
              (-> x
                d/->deferred
                (d/chain (fn [x] true))
                (d/catch (fn [_] false)))

              (or
                (identical? :lamina/closed! x)
                (identical? :lamina/error! x))
              (d/success-deferred false)

              :else
              (d/success-deferred true))]
      (if blocking?
        @x
        x)))

  (put [this x blocking? timeout timeout-val]

    (.put this x blocking?)))

(extend-protocol manifold/Sourceable

    lamina.core.channel.IChannel
    (to-source [ch]
      (->LaminaChannelSource ch)))

(extend-protocol manifold/Sinkable

    lamina.core.channel.IChannel
    (to-sink [ch]
      (->LaminaChannelSink ch)))
