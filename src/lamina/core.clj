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

(defn channel-pair
  "Returns a pair of channels, where all messages enqueued into one channel can
   be received by the other, and vice-versa."
  []
  (let [a (channel)
        b (channel)]
    [(splice a b) (splice b a)]))

(import-fn r/result-channel)
(import-fn r/result?)
(import-fn r/with-timeout)
(import-fn r/expiring-result)
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

(import-fn op/receive-in-order)

(defn error
  "Puts the channel or result-channel into an error state."
  [channel err]
  (if (result? channel)
    (r/error channel err)
    (ch/error channel err)))

(defn siphon
  "something goes here"
  [src dst]
  (if (result? src)
    (r/siphon-result src dst)
    (ch/siphon src dst)))

(defn join
  "something goes here"
  [src dst]
  (if (result? src)
    (do
      (r/siphon-result src dst)
      (r/subscribe dst (r/result-callback (fn [_]) #(r/error src %))))
    (ch/join src dst)))

(defn enqueue
  "Enqueues the message or messages into the channel."
  ([channel message]
     (u/enqueue channel message))
  ([channel message & messages]
     (u/enqueue channel message)
     (doseq [m messages]
       (u/enqueue channel m))))

(import-macro ch/read-channel*)

(import-fn ch/fork)

(import-fn ch/idle-result)
(import-fn ch/close-on-idle)

(import-fn ch/close)
(import-fn ch/drained?)
(import-fn ch/closed?)
(import-fn ch/transactional?)
(import-fn ch/on-closed)
(import-fn ch/on-drained)
(import-fn ch/closed-result)
(import-fn ch/drained-result)
(import-fn ch/cancel-callback)

(import-macro p/pipeline)
(import-macro p/run-pipeline)
(import-fn p/restart)
(import-fn p/redirect)
(import-fn p/complete)
(import-fn p/read-merge)
(import-macro p/wait-stage)

(import-fn ch/multiplexer)
(import-fn op/combine-latest)

(import-macro op/consume)
(import-fn ch/map*)
(import-fn ch/filter*)
(import-fn ch/remove*)
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

(defmacro channel->>
  "Creates a channel, pipes it through the ->> operator, and returns that same
   channel. This can be useful when defining :probes for an instrumented
   function, among other places.

   (channel->> (map* inc) (map* dec) (sink println))

   expands to

   (let [ch (channel)]
     (->> ch (map* inc) (map* dec) (sink println))
     ch)"
  [& transforms]
  (unify-gensyms
    `(let [ch## (channel)]
       ~(if (empty? transforms)
          `ch##
          `(do
             (->> ch## ~@transforms)
             ch##)))))

(defmacro split->>
  "Returns a channel which will forward each message to all downstream-channels.
   This can be used with channel->>, siphon->>, and join->> to define complex
   message flows:

   (join->> (map* inc)
     (split->>
       (channel->> (filter* even?) (sink log-even))
       (channel->> (filter* odd?) (sink log-odd))))"
  [& downstream-channels]
  `(let [ch# (channel)]
     (doseq [x# ~downstream-channels]
       (siphon ch## x#))
     ch#))

(defmacro siphon->>
  "A variant of channel->> where the last argument is assumed to be a channel,
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
  "A variant of channel->> where the last argument is assumed to be a channel,
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

(import-fn op/channel-seq)
(import-fn op/lazy-channel-seq)

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
