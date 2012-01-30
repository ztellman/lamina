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
    [lamina.core.channel :as ch]
    [lamina.core.pipeline :as p]
    [lamina.core.probe :as pr]
    [lamina.core.operators :as op]))

(import-fn ch/channel)
(import-fn ch/closed-channel)
(import-macro ch/channel*)
(import-fn ch/splice)

(defn channel-pair []
  (let [a (channel)
        b (channel)]
    [(splice a b) (splice b a)]))

(import-fn pr/probe-channel)
(import-fn pr/sympathetic-probe-channel)
(import-fn pr/probe-enabled?)

(import-fn ch/enqueue)
(import-fn ch/receive)
(import-fn ch/read-channel)
(import-macro ch/read-channel*)
(import-fn ch/receive-all)

(import-fn ch/siphon)
(import-fn ch/join)
(import-fn ch/fork)

(import-fn ch/close)
(import-fn ch/error)
(import-fn ch/drained?)
(import-fn ch/closed?)
(import-fn ch/on-closed)
(import-fn ch/on-drained)
(import-fn ch/on-error)
(import-fn ch/cancel-callback)

(import-macro p/pipeline)
(import-macro p/run-pipeline)
(import-fn p/restart)
(import-fn p/redirect)

(import-macro op/consume)
(import-fn ch/map*)
(import-fn ch/filter*)
(import-fn op/take*)
(import-fn op/take-while*)
(import-fn op/reductions*)
(import-fn op/reduce*)

(import-fn op/channel-seq)
(import-fn op/lazy-channel-seq)


;; what we had in previous version of lamina.core

(comment
  ;; core channel functions

(import-fn #'channel/channel?)
(import-fn #'channel/poll)

;; channel variants
(import-fn #'channel/permanent-channel)
(import-fn #'channel/closed-channel)
(import-fn #'channel/timed-channel)
(import-fn #'channel/proxy-channel)

(def nil-channel channel/nil-channel)

;; channel utility functions


(defmacro siphon->> [& forms]
  (let [ch-sym (gensym "ch")]
    `(let [~ch-sym (channel)]
       (apply siphon
	 ~(let [operators (butlast forms)]
	    (if (empty? operators)
	      ch-sym
	      `(->> ~ch-sym ~@operators)))
	 (let [dsts# ~(last forms)]
	   (if (coll? dsts#) dsts# [dsts#])))
       ~ch-sym)))

(defn sink [& callbacks]
  (let [ch (channel)]
    (apply receive-all ch callbacks)
    ch))

(import-fn #'seq/remove*)
(import-fn #'seq/receive-in-order)
(import-fn #'seq/reduce*)
(import-fn #'seq/reductions*)
(import-fn #'seq/partition*)
(import-fn #'seq/partition-all*)

(import-fn #'op/sample-every)

;; named channels
(import-fn #'named/named-channel)
(import-fn #'named/release-named-channel)

;; synchronous channel functions
(import-fn #'seq/lazy-channel-seq)
(import-fn #'seq/channel-seq)
(import-fn #'seq/wait-for-message)


;;;; PIPELINES

;; core pipeline functions
(import-fn #'pipeline/result-channel)
(import-fn #'pipeline/pipeline)
(import-fn #'pipeline/run-pipeline)

;; pipeline stage helpers
(import-fn #'pipeline/result-channel?)
(import-fn #'pipeline/read-channel)
(import-fn #'pipeline/read-merge)

(import-fn #'pipeline/on-success)
(import-fn #'pipeline/on-error)

(defmacro do-stage
  "Creates a pipeline stage that emits the same value it receives, but performs some side-effect
   in between.  Useful for debug prints and logging."
  [& body]
  `(fn [x#]
     ~@body
     x#))

(import-fn #'pipeline/wait-stage)

;; redirect signals
(import-fn #'pipeline/redirect)
(import-fn #'pipeline/restart)
(import-fn #'pipeline/complete)

;; pipeline result hooks
(import-fn #'pipeline/wait-for-result)
(import-fn #'pipeline/siphon-result))
