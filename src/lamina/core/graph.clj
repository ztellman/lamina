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
    [lamina.core.threads :only (enqueue-cleanup)])
  (:require
    [lamina.core.utils :as u]
    [lamina.core.result :as r]
    [lamina.time :as t]
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



;;;

(import-fn c/edge)
(import-fn c/propagate)
(import-fn c/close)
(import-fn c/transactional)
(import-fn c/downstream)
(import-fn c/downstream-propagators)

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

(import-fn n/connect)
(import-fn n/siphon)
(import-fn n/join)

(import-fn p/bridge)
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
    :transactional? (transactional? node)))

;;;

(defn on-event [result-fn]
  (fn [node callback]
    (r/subscribe (result-fn node callback)
      (r/result-callback
        (fn [_] (callback))
        (fn [_] (callback))))))

(def on-closed (on-event closed-result))
(def on-drained (on-event drained-result))

(defn on-error [node callback]
  (r/subscribe (error-result node callback)
    (r/result-callback
      (fn [_])
      (fn [err] (callback err)))))

;;;

(defmacro read-node*
  [n & {:keys [predicate
               timeout
               result
               listener-result
               on-timeout
               on-false
               on-drained
               task-queue]
        :or {task-queue 'lamina.time/default-task-queue}
        :as options}]
  (let [timeout? (contains? options :timeout)]
    (unify-gensyms
     `(let [result## (read-node ~n
                       ~predicate
                       ~(when predicate
                          (or on-false :lamina/false))
                       ~result)
            timeout-latch## ~(when timeout?
                               `(atom false))
            cancel-timeout## ~(when timeout?
                                `(let [timeout# ~timeout]
                                   (when (and timeout# (instance? ResultChannel result##))
                                     (t/invoke-in ~task-queue timeout#
                                       (fn []
                                         (reset! timeout-latch## true)
                                         ~(if on-timeout
                                            `(r/success result## ~on-timeout)
                                            `(u/error result## :lamina/timeout! false)))))))]
       
        ;; downstream results for listeners
        ~@(when (contains? options :listener-result)
            `((r/enqueue-to-listeners
                (r/listeners result##)
                ~listener-result)))

        ;; drained result
        ~(if-not (contains? options :on-drained)
           `result##
           `(if (instance? SuccessResult result##)
              result##
              (let [result# (r/result-channel)]
                (r/subscribe result##
                  (r/result-callback
                    (fn [x#]
                      (when (and timeout-latch## (not @timeout-latch##))
                        (cancel-timeout##))
                      (r/success result# x#))
                    (fn [err#]
                      (when (and timeout-latch## (not @timeout-latch##))
                        (cancel-timeout##))
                      (if (identical? :lamina/drained! err#)
                        (r/success result# ~on-drained)
                        (u/error result# err# false)))))
                result#)))))))
