;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns
  ^{:author "Zachary Tellman"}
  lamina.core
  (:use
    [potemkin :only (import-fn)])
  (:require
    [lamina.core.pipeline :as pipeline]
    [lamina.core.channel :as channel]
    [lamina.core.seq :as seq]
    [lamina.core.named :as named]
    [lamina.core.expr :as x])
  (:import))


;;;; CHANNELS

;; core channel functions
(import-fn channel/receive)
(import-fn channel/cancel-callback)
(import-fn channel/enqueue)
(import-fn channel/enqueue-and-close)
(import-fn channel/close)
(import-fn channel/on-closed)
(import-fn channel/on-drained)
(import-fn channel/drained?)
(import-fn channel/closed?)
(import-fn channel/channel?)
(import-fn seq/receive-all)
(import-fn channel/poll)

;; channel variants
(import-fn channel/splice)
(import-fn channel/channel)
(import-fn channel/channel-pair)
(import-fn channel/permanent-channel)
(import-fn channel/constant-channel)
(import-fn channel/closed-channel)
(import-fn channel/timed-channel)

(def nil-channel channel/nil-channel)

;; channel utility functions

(defn siphon
  [ch & dsts]
  (seq/siphon ch (zipmap dsts (repeat (count dsts) identity))))

(import-fn seq/fork)
(import-fn seq/map*)
(import-fn seq/filter*)
(import-fn seq/receive-in-order)
(import-fn seq/reduce*)
(import-fn seq/reductions*)
(import-fn seq/take*)
(import-fn seq/take-while*)

;; named channels
(import-fn named/named-channel)
(import-fn named/release-named-channel)

;; synchronous channel functions
(import-fn seq/lazy-channel-seq)
(import-fn seq/channel-seq)
(import-fn seq/wait-for-message)


;;;; PIPELINES

;; core pipeline functions
(import-fn pipeline/result-channel)
(import-fn pipeline/pipeline)
(import-fn pipeline/run-pipeline)

;; pipeline stage helpers
(import-fn pipeline/result-channel?)
(import-fn pipeline/read-channel)
(import-fn pipeline/read-merge)
(import-fn pipeline/blocking)

(defmacro do*
  "Creates a pipeline stage that emits the same value it receives, but performs some side-effect
   in between.  Useful for debug prints and logging."
  [& body]
  `(fn [x#]
     ~@body
     x#))

(defmacro wait
  "Creates a pipeline stage that accepts a value, and emits the same value after 'interval' milliseconds."
  [interval]
  `(fn [x#]
     (run-pipeline
       (let [interval# ~interval]
	 (when (pos? interval#)
	   (read-channel (timed-channel interval#))))
       (fn [_#] x#))))

;; redirect signals
(import-fn pipeline/redirect)
(import-fn pipeline/restart)
(import-fn pipeline/complete)

;; pipeline result hooks
(import-fn pipeline/wait-for-result)
(import-fn pipeline/siphon-result)

;;;

(import-fn x/set-default-executor)
(import-fn x/set-local-executor)

(defmacro async
  "Performs magic.

   Any expression in a block wrapped by (async ...) can use a result-channel instead of an
   actual value, and defer its own execution (and the execution of all code that depends on
   its value) until the result-channel has emitted a value.  The value returned from an
   (async ...) block is always a result-channel.

   This means that we can write code that looks like a normal function, but is actually several
   distinct callbacks stitched together.  Consider a situation where we want to read two messages
   from a channel.  We could compose nested callbacks:

   (receive ch
     (fn [first-message]
       (receive ch
         (fn [second-message]
           (perform-action [first-message second-message])))))

   However, using async and read-channel (which returns a result-channel representing the next
   message in the channel), this becomes a lot more straightforward:

   (async
     (let [first-message (read-channel ch)
           second-message (read-channel ch)]
       [first-message second-message]))

   This will return a result-channel which will emit a vector of the next two messages from the
   channel, once they've both arrived.

   This is very, very experimental, and may be subject to change."
  [& body]
  (x/async body))

(defmacro future*
  "A variation of 'future' that returns a result-channel instead of a synchronous
   future object."
  [& body]
  (x/future* body))
