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
    [lamina.core.result :as r]
    [lamina.core.pipeline :as p]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.lock
     Lock]
    [java.util.concurrent.atomic
     AtomicInteger
     AtomicBoolean]))

(set! *warn-on-reflection* true)

;;;

;; TODO: think about race conditions with closing the destination channel while a message is en-route
;; hand-over-hand locking in the node when ::consumed?

(defmacro consume
  "Consumes messages one-by-one from the source channel, per the read-channel* style parameters.
   Valid options are:

   :message-predicate - a function that takes a message, and returns true if it should be consumed
   :while-predicate - a no-arg function that returns true if another message should be read
   :message-transform -
   :value-transform -
   :initial-value -
   :timeout - a no-arg function that returns the maximum time that should be spent waiting for the next message
   :description - a description of the consumption mechanism
   :channel - the destination channel for the messages

   If the predicate returns false or the timeout elapses, the consumption will cease."
  [ch & {:keys [message-predicate
                while-predicate
                timeout
                description
                channel
                reducer
                initial-value
                message-transform]}]
  (let [dst-node-sym (gensym "node")
        message-predicate-sym (gensym "message-predicate")
        while-predicate-sym (gensym "while-predicate")
        message-transform-sym (gensym "message-transform-sym")
        msg-sym (gensym "msg")
        timeout-sym (gensym "timeout")
        src-channel-sym (gensym "src")
        reducer-sym (gensym "reducer")
        val-sym (gensym "val")
        msg-sym (gensym "msg")]
    `(let [~src-channel-sym ~ch
           dst# ~(or channel `(c/channel* :transactional? (c/transactional? ~src-channel-sym)))
           ~dst-node-sym (c/receiver-node dst#) 
           initial-val# ~initial-value
           ~message-predicate-sym ~message-predicate
           ~message-predicate-sym ~(if message-predicate
                                     `(fn [~@(when reducer `(val-sym)) x#]
                                        (and
                                          (not (n/closed? ~dst-node-sym))
                                          (~message-predicate-sym
                                            ~@(when reducer `(val-sym))
                                            x#)))
                                     `(fn [~'& _#]
                                        (not (n/closed? ~dst-node-sym))))
           ~while-predicate-sym ~while-predicate
           ~message-transform-sym ~message-transform
           ~reducer-sym ~reducer
           ~timeout-sym ~timeout]
       (if-let [unconsume# (n/consume
                             (c/emitter-node ~src-channel-sym)
                             (n/edge
                               ~(or description "consume")
                               (c/receiver-node dst#)))]

         (do
           (p/run-pipeline initial-val#
             {:error-handler (fn [ex#]
                               (log/error ex# "error in consume")
                               (c/error dst# ex#))}
             (fn [~val-sym]
               (if-not ~(if while-predicate
                          `(~while-predicate-sym ~@(when reducer `(~val-sym)))
                          true)
                 ::close
                 (p/run-pipeline
                   (c/read-channel* ~src-channel-sym
                     :on-false ::close
                     :on-timeout ::timeout
                     :on-drained ::close
                     ~@(when timeout `(:timeout (~timeout-sym)))
                     :predicate ~(if reducer
                                   `(fn [msg#]
                                      (~message-predicate-sym ~@(when reducer `(~val-sym)) msg#))
                                   message-predicate-sym))
                   (fn [~msg-sym]
                     (if (identical? ~msg-sym ::close)
                       ::close
                       (let [~val-sym ~(when reducer `(~reducer-sym ~val-sym ~msg-sym))]
                         (c/enqueue dst#
                           ~(if message-transform
                              `(~message-transform-sym ~@(when reducer `(~val-sym)) ~msg-sym)
                              msg-sym))
                         ~val-sym))))))
             (fn [val#]
               (if (identical? val# ::close)
                 (do
                   (unconsume#)
                   (c/close dst#))
                 (p/restart val#))))
           dst#)

         ;; something's already attached to the source
         (do
           (c/error dst# (IllegalStateException. "Can't consume, channel already in use."))
           dst#)))))

;;;

(defn last* [ch]
  (let [val (atom nil)]
    (c/receive-all ch #(reset! val %))
    (p/run-pipeline ch
      c/drained-result
      (fn [_] @val))))

(defn take* [n ch]
  (consume ch
    :description (str "take* " n)
    :initial-value 0
    :reducer (fn [n _] (inc n))
    :while-predicate #(< % n)))

(defn take-while* [f ch]
  (consume ch
    :description "take-while*"
    :message-predicate f))

(defn reductions- [f val ch ch*]
  (let [ch* (or
              ch*
              (c/channel*
               :transactional? (c/transactional? ch)
               :messages [val]))]
    (consume ch
      :channel ch*
      :description "reductions*"
      :initial-value val
      :reducer f
      :message-transform (fn [v _] v))))

(defn reductions*
  ([f ch]
     (let [ch* (c/channel*
                 :transactional? (c/transactional? ch))]
       (p/run-pipeline ch
         c/read-channel
         #(do
            (c/enqueue ch* %)
            (reductions- f % ch ch*)))
       ch*))
  ([f val ch]
     (reductions- f val ch nil)))

(defn reduce*
  ([f ch]
     (last* (reductions* f ch)))
  ([f val ch]
     (last* (reductions* f val ch))))

;;;

(defn lazy-channel-seq-
  [read-fn cleanup-fn]
  (let [msg @(read-fn)]
    (if (= ::end msg)
      (do
        (cleanup-fn)
        nil)
      (cons msg (lazy-seq (lazy-channel-seq- read-fn cleanup-fn))))))

(defn lazy-channel-seq
  ([ch]
     (lazy-channel-seq ch -1))
  ([ch timeout]
     (let [timeout-fn (if (number? timeout)
                        (when-not (neg? timeout)
                          (constantly timeout))
                        timeout)
           e (n/edge "lazy-channel-seq" (n/terminal-node nil))]
       (if-let [unconsume (n/consume (c/emitter-node ch) e)]
         (lazy-channel-seq-
           (if timeout-fn
             #(c/read-channel* ch :timeout (timeout-fn) :on-timeout ::end :on-drained ::end)
             #(c/read-channel* ch :on-drained ::end))
           unconsume)
         (throw (IllegalStateException. "Can't consume, channel already in use."))))))

(defn channel-seq
  ([ch]
     (n/ground (c/emitter-node ch)))
  ([ch timeout]
     (let [start (System/currentTimeMillis)
           s (n/ground (c/emitter-node ch))]
       (concat s
         (lazy-channel-seq ch
           #(max 0 (- timeout (- (System/currentTimeMillis) start))))))))
