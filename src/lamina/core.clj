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
  (:import
    [java.util.concurrent
     TimeoutException]))


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
(import-fn channel/proxy-channel)

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
(def read-channel* read-channel)
(import-fn pipeline/read-merge)

(import-fn pipeline/on-success)
(import-fn pipeline/on-error)

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

(defmacro task
  "A variation of 'future' that returns a result-channel instead of a synchronous
   future object.

   When used within (async ...), it's simply an annotation that the body should be executed
   on a separate thread."
  [& body]
  (x/transform-task body))

;;;

(import-fn x/compact)

(defmacro force-all [expr]
  `(~'force (compact ~expr)))

(defmacro async
  "Turns standard Clojure expressions into a dataflow representation of the computation.

   Any result-channel can be treated as a real value inside the async block.  The value
   returned by the async block will be a result-channel."
  [& body]
  (x/async body))

(defmacro defn-async
  "Creates an asynchronous function.  Equivalent to (async (defn ....))."
  [& body]
  `(def ~(first body)
     (deref (async (fn ~(first body) ~@(rest body))))))

(defmacro with-timeout
  "Wraps a body that returns a result-channel, and returns a new result-channel that will
   emit a java.util.concurrent.TimeoutException if the inner result-channel doesn't yield
   a value in 'timeout' ms."
  [timeout & body]
  (let [start-sym (gensym "start")]
    `(let [~start-sym (System/currentTimeMillis)
	   result# (do ~@body)]
       (if-not (result-channel? result#)
	 result#
	 (let [result## (result-channel)]
	   (receive
	     (pipeline/poll-result result#
	       ~(cond
		  (zero? timeout) 0
		  (neg? timeout) -1
		  :else `(max 1 (- ~timeout (- (System/currentTimeMillis) ~start-sym)))))
	     (fn [poll-result#]
	       (if-not poll-result#
		 (pipeline/error! result## (TimeoutException. "Timed out waiting for async result."))
		 (let [[outcome# value#] poll-result#]
		   (condp = outcome#
		     :success (pipeline/success! result## value#)
		     :error (pipeline/error! result## value#))))))
	   result##)))))
