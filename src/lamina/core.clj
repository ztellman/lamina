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
  []
  (channel* :permanent? true))

(defn channel-pair
  "Returns a pair of channels, where all messages enqueued into one channel can be received by the other,
   and vice-versa."
  []
  (let [a (channel)
        b (channel)]
    [(splice a b) (splice b a)]))

(import-fn r/result-channel)
(import-fn r/result-channel?)
(import-fn r/with-timeout)
(import-fn r/expiring-result)
(import-fn r/success)
(import-fn r/success-result)
(import-fn r/error-result)

;;;

(defn on-result
  "Allows two callbacks to be registered on a result-channel, one in the case of a result, the other in case of
   an error.

   This is usually preferable to just using on-success or on-error in isolation, but often can and should be replaced
   by a pipeline."
  [result-channel on-success on-error]
  (r/subscribe result-channel (r/result-callback on-success on-error)))

(defn on-success
  "Allows a callback to be registered on a result-channel which will be invoked with the result when it is realized.
   If the result-channel emits an error, the callback will not be called.

   This almost always can and should be replaced by a pipeline."
  [result-channel callback]
  (r/subscribe result-channel (r/result-callback callback (fn [_]))))

(defn on-error
  "Allows a callback to be registered on a result-channel which will be invoked with the error if
   it is realized as an error.

   This often can and should be replaced by a pipeline."
  [channel callback]
  (if (result-channel? channel)
    (r/subscribe channel (r/result-callback (fn [_]) callback))
    (ch/on-error channel callback)))

;;;

(import-fn ch/receive)
(import-fn ch/receive-all)
(import-fn ch/read-channel)

(import-fn ch/sink)

(import-fn op/receive-in-order)

(defn error
  "Puts the channel or result-channel into an error state."
  [channel err]
  (if (result-channel? channel)
    (r/error channel err)
    (ch/error channel err)))

(defn siphon
  "something goes here"
  [src dst]
  (if (result-channel? src)
    (r/siphon-result src dst)
    (ch/siphon src dst)))

(defn join
  "something goes here"
  [src dst]
  (if (result-channel? src)
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

(defmacro split->> [& downstream-channels]
  `(let [ch# (channel)]
     (doseq [x# ~downstream-channels]
       (siphon ch## x#))
     ch#))

(defmacro siphon->> [& transforms+downstream-channel]
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

(defmacro join->> [& transforms+downstream-channel]
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


;; what we had in previous version of lamina.core

(comment
  ;; core channel functions

;; channel variants

;; named channels
(import-fn #'named/named-channel)
(import-fn #'named/release-named-channel)

)
