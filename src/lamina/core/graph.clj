;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.graph
  (:use
    [potemkin]
    [useful.datatypes :only (make-record assoc-record)]
    [lamina.core.threads :only (enqueue-cleanup)]
    [lamina.core utils])
  (:require
    [lamina.core.result :as r]
    [lamina.core.threads :as t]
    [lamina.core.graph.core :as c]
    [lamina.core.graph.node :as n]
    [lamina.core.graph.propagator :as p])
  (:import
    [lamina.core.result
     ResultChannel
     SuccessResult]
    [lamina.core.graph.node
     NodeState
     Node]))

(set! *warn-on-reflection* true)

;;;

(import-fn c/edge)
(import-fn c/propagate)
(import-fn c/error)
(import-fn c/close)
(import-fn c/transactional)
(import-fn c/downstream)
(import-fn c/downstream-nodes)

(import-fn n/state)
(import-fn n/link)
(import-fn n/receive)
(import-fn n/read-node)
(import-fn n/consume)
(import-fn n/split)
(import-fn n/cancel)
(import-fn n/on-state-changed)
(import-fn n/drain)

(import-fn n/queue)
(import-fn n/closed?)
(import-fn n/drained?)
(import-fn n/split?)
(import-fn n/consumed?)
(import-fn n/transactional?)
(import-fn n/permanent?)
(import-fn n/grounded?)
(import-fn n/closed-result)
(import-fn n/drained-result)
(import-fn n/error-result)
(import-fn n/error-value)

(import-fn n/node)
(import-macro n/node*)
(import-fn n/node?)

(import-fn n/siphon)
(import-fn n/join)

(import-fn p/bridge-siphon)
(import-fn p/bridge-join)
(import-fn p/terminal-propagator)
(import-fn p/callback-propagator)
(import-fn p/distributing-propagator)

;;;

(defn ground [n]
  (let [e (edge "" (terminal-propagator "\u23DA"))]
    (link n ::ground e nil nil)))

(defn downstream-node [operator ^Node upstream-node]
  (let [^Node n (node operator)]
    (join upstream-node n)
    n))

(defn mimic [node]
  (node*
    :transactional? (transactional? node)
    :permanent? (permanent? node)
    :grounded? (grounded? node)))

;;;

(defn on-event [result-fn]
  (fn [node callback]
    (r/subscribe (result-fn node)
      (r/result-callback
        (fn [_] (callback))
        (fn [_] (callback))))))

(def on-closed (on-event closed-result))
(def on-drained (on-event drained-result))

(defn on-error [node callback]
  (r/subscribe (error-result node)
    (r/result-callback
      (fn [_])
      (fn [err] (callback err)))))

;;;

(defmacro read-node*
  [n & {:keys [predicate
               timeout
               result
               on-timeout
               on-false
               on-drained]
        :as options}]
  (let [result-sym (gensym "result")]
    `(let [~result-sym (read-node ~n
                         ~predicate
                         ~(when predicate
                            (or on-false :lamina/false))
                         ~result)]
       ~@(when (contains? options :timeout)
           `((let [timeout# ~timeout]
               (when (and timeout# (instance? ResultChannel ~result-sym))
                 (t/delay-invoke timeout#
                   (fn []
                     ~(if on-timeout
                        `(r/success ~result-sym ~on-timeout)
                        `(r/error ~result-sym :lamina/timeout!))))))))
       ~(if-not (contains? options :on-drained)
          result-sym
          `(if (instance? SuccessResult ~result-sym)
             ~result-sym
             (let [result# (r/result-channel)]
               (r/subscribe ~result-sym
                 (r/result-callback
                   (fn [x#] (r/success result# x#))
                   (fn [err#]
                     (if (identical? :lamina/drained! err#)
                       (r/success result# ~on-drained)
                       (r/error result# err#)))))
               result#))))))
