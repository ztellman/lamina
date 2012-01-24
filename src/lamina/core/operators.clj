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
   :timeout - a no-arg function that returns the maximum time that should be spent waiting for the next message
   :description - a description of the consumption mechanism
   :channel - the destination channel for the messages

   If the predicate returns false or the timeout elapses, the consumption will cease."
  [ch & {:keys [message-predicate while-predicate timeout description channel]}]
  (let [node-sym (gensym "node")
        message-predicate-sym (gensym "message-predicate")
        while-predicate-sym (gensym "while-predicate")
        msg-sym (gensym "msg")
        timeout-sym (gensym "timeout")
        src-sym (gensym "src")]
    `(let [~src-sym ~ch
           dst# ~(or channel `(c/channel* :transactional? (c/transactional? ~src-sym)))
           ~node-sym (c/receiver-node dst#)
           ~message-predicate-sym ~(if message-predicate
                                     `(fn [x#]
                                        (and
                                          (not (n/closed? ~node-sym))
                                          (~message-predicate x#)))
                                     `(fn [_#] (not (n/closed? ~node-sym))))
           ~while-predicate-sym ~while-predicate
           ~timeout-sym ~timeout]
       (if-let [unconsume# (n/consume
                             (c/emitter-node ~src-sym)
                             (n/edge
                               ~(or description "consume")
                               (c/receiver-node dst#)))]

         (do
           (p/run-pipeline ~src-sym
             {:error-handler (fn [ex#]
                               (log/error ex# "error in consume")
                               (c/error dst# ex#))
              :result? false}
             (fn [ch#]
               (if-not ~(if while-predicate `(~while-predicate-sym) true)
                 ::close
                 (c/read-channel* ch#
                   :on-false ::close
                   :on-timeout ::close
                   :on-drained ::close
                   ~@(when timeout `(:timeout (~timeout-sym)))
                   :predicate ~message-predicate-sym)))
             (fn [msg#]
               (if (identical? msg# ::close)
                 (do
                   (unconsume#)
                   (c/close dst#))
                 (do
                   (c/enqueue dst# msg#)
                   (p/restart)))))
           dst#)

         ;; something's already attached to the source
         (throw (IllegalStateException. "Can't consume, channel already in use."))))))

;;;

(defprotocol ISemiTransactional
  (transactional-copy [_]))

(defn semi-transactional [f x]
  (let [latch (AtomicBoolean. false)
        x* (ref nil)]
    (fn [& args]
      (if (clojure.lang.LockingTransaction/isRunning)
        (do
          (when (.compareAndSet latch false true)
            (ref-set x* (transactional-copy x)))
          (apply f @x* args))
        (if (.get latch)
          (dosync (apply f @x* args))
          (apply f x args))))))

;;;

(defprotocol ICounter
  (increment [_]))

(deftype TransactionalCounter [n]
  ICounter
  (increment [_] (alter n inc)))

(deftype Counter [^AtomicInteger n]
  ISemiTransactional
  (transactional-copy [_] (TransactionalCounter. (ref (.get n))))
  ICounter
  (increment [_] (.incrementAndGet n)))

(defn take* [n ch]
  (let [increment (semi-transactional
                    increment
                    (Counter. (AtomicInteger. 0)))]
    (consume ch
      :description (str "take " n)
      :while-predicate #(<= (increment) n))))

(defn take-while* [f ch]
  (consume ch
    :description "take-while*"
    :message-predicate f))

;;;

(defprotocol IReducer
  (update-reduction [_ val])
  (current-reduction [_]))

(deftype Reducer
  [^Lock lock
   reduce-fn
   callback
   ^{:volatile-mutable true} value]
  IReducer
  (update-reduction [_ val]
    (let [value (l/with-exclusive-lock* lock
                  (set! value (reduce-fn value val)))]
      (when callback
        (callback value))
      value))
  (current-reduction [_]
    value))

(defn reducer [f val callback]
  (Reducer. (l/lock) f callback val))

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
