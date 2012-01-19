;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.operators
  (:require
    [lamina.core.node :as n]
    [lamina.core.channel :as c]
    [lamina.core.lock :as l]
    [lamina.core.pipeline :as p]))

;;;

(defmacro consume
  "Consumes messages one-by-one from the source channel, per the read-channel* style parameters.
   Valid options are:

   :predicate - a function that takes a message, and returns true if it should be consumed
   :timeout - a no-arg function that returns the maximum time that should be spent waiting for the next message
   :description - a description of the consumption mechanism
   :channel - the destination channel for the messages

   If the predicate returns false or the timeout elapses, the consumption will cease."
  [ch & {:keys [predicate timeout description channel]}]
  (let [node-sym (gensym "node")
        predicate-sym (gensym "predicate")
        msg-sym (gensym "msg")
        timeout-sym (gensym "timeout")]
    `(let [src# ~ch
           dst# ~(or channel `(c/channel))
           ~node-sym (c/receiver-node src#)
           ~predicate-sym ~predicate
           ~timeout-sym ~timeout]
       (if-not (n/consume
                 (c/emitter-node src#)
                 dst#
                 (n/edge
                   ~(or description "consume")
                   (c/receiver-node dst#)))
         (throw (IllegalStateException. "Can't consume, channel already in use."))
         (do
           (p/run-pipeline src#
             {:error-handler (fn [ex#] (c/error dst# ex#))}
             (fn [ch#]
               (c/read-channel* ch#
                 :on-false ::close
                 :on-timeout ::close
                 :on-drained ::close
                 ~@(when timeout `(:timeout (~timeout-sym)))
                 :predicate (fn [~msg-sym]
                              (l/acquire ~node-sym)
                              ~(if-not predicate
                                 true
                                 `(if (~predicate-sym ~msg-sym)
                                    true
                                    (do
                                      (l/release ~node-sym)
                                      false))))))
             (fn [msg#]
               (if (identical? msg# ::close)
                 (do
                   (n/cancel (c/emitter-node src#) dst#)
                   (c/close dst#))
                 (do
                   (c/enqueue dst# msg#)
                   (l/release ~node-sym)
                   (p/restart)))))
           dst#)))))

;;;

;; TODO: make this transactionally safe
(defn take* [n ch]
  (let [cnt (atom 0)]
    (consume ch
      :description (str "take " n)
      :predicate (fn [_] (<= (swap! cnt inc) n)))))

(defn take-while* [f ch]
  (consume ch
    :predicate f))

