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
    [lamina.core.watch :as w]
    [lamina.core.named :as n]
    [lamina.core.utils :as u]
    [lamina.core.channel :as ch]
    [lamina.core.pipeline :as p]
    [lamina.core.result :as r]
    [lamina.core.operators :as op]
    [clojure.tools.logging :as log]))

;;;

(import-fn ch/channel)
(import-fn ch/closed-channel)
(import-fn ch/grounded-channel)
(import-macro ch/channel*)
(import-fn ch/splice)
(import-fn ch/channel?)
(import-fn ch/ground)

(defn permanent-channel
  "Returns a channel which cannot be closed or put into an error state."
  [& messages]
  (channel* :permanent? true
            :messages (seq messages)))

(import-fn r/result-channel)
(import-fn r/async-result?)
(import-fn r/with-timeout)
(import-fn r/expiring-result)
(import-fn r/timed-result)
(import-fn r/success)
(import-fn r/success-result)
(import-fn r/error-result)
(import-fn r/merge-results)

;;;

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

(import-fn ch/receive)
(import-fn ch/receive-all)
(import-fn ch/read-channel)

(import-fn ch/sink)

(import-fn w/atom-sink)
(import-fn w/watch-channel)

(import-fn op/receive-in-order)

(defn error
  "Puts the channel or result-channel into an error state."
  [channel err]
  (if (async-result? channel)
    (r/error channel err)
    (ch/error channel err)))

(defn siphon
  "something goes here"
  ([src dst]
     (if (async-result? src)
       (r/siphon-result src dst)
       (ch/siphon src dst)))
  ([src dst & rest]
     (siphon src dst)
     (apply siphon dst rest)))

(defn join
  "something goes here"
  ([src dst]
     (if (async-result? src)
       (do
         (r/siphon-result src dst)
         (r/subscribe dst (r/result-callback (fn [_]) #(r/error src %))))
       (ch/join src dst)))
  ([src dst & rest]
     (join src dst)
     (apply join dst rest)))

(defn enqueue
  "Enqueues the message or messages into the channel."
  ([channel message]
     (u/enqueue channel message))
  ([channel message & messages]
     (let [val (u/enqueue channel message)]
       (doseq [m messages]
         (u/enqueue channel m))
       val)))

(import-macro ch/read-channel*)

(import-fn ch/fork)
(import-fn ch/tap)

(import-fn ch/idle-result)
(import-fn ch/close-on-idle)

(import-fn ch/close)
(import-fn ch/drained?)
(import-fn ch/closed?)
(import-fn ch/transactional?)
(import-fn ch/on-closed)
(import-fn ch/on-drained)
(import-fn ch/on-error)
(import-fn ch/closed-result)
(import-fn ch/drained-result)
(import-fn ch/cancel-callback)

(defn enqueue-and-close
  "something goes here"
  [ch & messages]
  (apply enqueue ch messages)
  (close ch))

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

(import-macro p/pipeline)
(import-macro p/run-pipeline)
(import-fn p/restart)
(import-fn p/redirect)
(import-fn p/complete)
(import-fn p/read-merge)
(import-macro p/wait-stage)

(import-fn ch/distributor)
(import-fn ch/aggregate)
(import-fn ch/distribute-aggregate)
(import-fn op/combine-latest)

(import-macro op/consume)
(import-fn ch/map*)
(import-fn ch/filter*)
(import-fn ch/remove*)
(import-fn op/mapcat*)
(import-fn op/concat*)
(import-fn op/take*)
(import-fn op/take-while*)
(import-fn op/reductions*)
(import-fn op/reduce*)
(import-fn op/last*)
(import-fn op/partition*)
(import-fn op/partition-all*)

(import-fn op/sample-every)
(import-fn op/partition-every)
(import-fn op/periodically)

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

(import-fn op/channel->seq)
(import-fn op/channel->lazy-seq)

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
          (log/error e "Error in lazy-seq->channel."))
        (finally
          (close ch))))

    ch))

(defn wait-for-message
  ([ch]
     @(read-channel ch))
  ([ch timeout]
     @(read-channel* ch :timeout timeout)))

(defn wait-for-result
  ([result-channel]
     @result-channel)
  ([result-channel timeout]
     @(with-timeout timeout result-channel)))

;;;

(import-fn n/named-channel)
(import-fn n/release-named-channel)
