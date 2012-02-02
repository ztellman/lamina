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
    [lamina.core.probe :as pr]
    [lamina.core.result :as r]
    [lamina.core.operators :as op]))

;;;

(import-fn ch/channel)
(import-fn ch/closed-channel)
(import-macro ch/channel*)
(import-fn ch/splice)
(import-fn ch/channel?)

(defn permanent-channel []
  (channel* :permanent? true))

(import-fn pr/probe-channel)
(import-fn pr/sympathetic-probe-channel)
(import-fn pr/probe-enabled?)

(defn channel-pair []
  (let [a (channel)
        b (channel)]
    [(splice a b) (splice b a)]))

(import-fn r/result-channel)
(import-fn r/result-channel?)

;;;

(defn on-result [result-channel on-success on-error]
  (r/subscribe result-channel (r/result-callback on-success on-error)))

(defn on-success [result-channel callback]
  (r/subscribe result-channel (r/result-callback callback (fn [_]))))

(defn on-error [channel callback]
  (if (result-channel? channel)
    (r/subscribe result-channel (r/result-callback (fn [_]) callback))
    (ch/on-error channel callback)))

;;;

(defn receive [channel callback]
  (if (result-channel? channel)
    (on-success channel callback)
    (ch/receive channel callback)))

(defn read-channel [channel]
  (if (result-channel? channel)
    channel
    (ch/read-channel channel)))

(defn receive-all [channel callback]
  (if (result-channel? channel)
    (on-success channel callback)
    (ch/receive-all channel callback)))

(defn error [channel err]
  (if (result-channel? channel)
    (r/error channel err)
    (ch/error channel err)))

(defn siphon [src dst]
  (if (result-channel? src)
    (r/siphon-result src dst)
    (ch/siphon src dst)))

(defn join [src dst]
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

(import-fn ch/siphon)
(import-fn ch/join)
(import-fn ch/fork)

(import-fn ch/close)
(import-fn ch/drained?)
(import-fn ch/closed?)
(import-fn ch/on-closed)
(import-fn ch/on-drained)
(import-fn ch/cancel-callback)

(import-macro p/pipeline)
(import-macro p/run-pipeline)
(import-fn p/restart)
(import-fn p/redirect)
(import-fn p/complete)
(import-fn p/read-merge)

(import-macro op/consume)
(import-fn ch/map*)
(import-fn ch/filter*)
(import-fn op/take*)
(import-fn op/take-while*)
(import-fn op/reductions*)
(import-fn op/reduce*)
(import-fn op/last*)

(defn remove* [f ch]
  (filter* (complement f) ch))

(import-fn op/channel-seq)
(import-fn op/lazy-channel-seq)



;; what we had in previous version of lamina.core

(comment
  ;; core channel functions

(import-fn #'channel/poll)

;; channel variants
(import-fn #'channel/timed-channel)

(def nil-channel channel/nil-channel)

;; channel utility functions


(defmacro siphon->> [& forms]
)

(defn sink [& callbacks]
  (let [ch (channel)]
    (apply receive-all ch callbacks)
    ch))

(import-fn #'seq/receive-in-order)
(import-fn #'seq/partition*)
(import-fn #'seq/partition-all*)

(import-fn #'op/sample-every)

;; named channels
(import-fn #'named/named-channel)
(import-fn #'named/release-named-channel)

;; synchronous channel functions
(import-fn #'seq/wait-for-message)


;;;; PIPELINES

(import-fn #'pipeline/wait-stage)

;; pipeline result hooks
(import-fn #'pipeline/wait-for-result)
(import-fn #'pipeline/siphon-result))
