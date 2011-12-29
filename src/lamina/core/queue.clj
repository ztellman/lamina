;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.queue
  (:require
    [lamina.core.result :as r]
    [lamina.core.lock :as l])
  (:import
    [lamina.core.lock AsymmetricLock]
    [lamina.core.result ResultChannel]
    [java.util.concurrent ConcurrentLinkedQueue CancellationException]
    [java.util.concurrent.atomic AtomicReference]
    [java.util ArrayList]))

(set! *warn-on-reflection* true)

(deftype MessageConsumer [predicate false-value ^ResultChannel result-channel])

(deftype Consumption [consumed? error? result result-channel])

;; This queue is specially designed to interact with the node in lamina.core.node, and
;; is not intended as a general-purpose data structure.
(defprotocol QueueProtocol
  (error [_ error]
    "All pending receives are resolved as errors. It's expected that the queue will
     be swapped out for an error-emitting queue at this point.") 
  (drained? [_]
    "Returns true if the queue is closed and empty.")
  (ground [_]
    "Clears and returns all messages currently in the queue.")
  (enqueue [_ msg persist? release-fn]
    "Enqueues a message into the queue. If 'persist?' is false, the message will only
     be sent to pending receivers, and not remain in the queue. The 'release-fn' callback,
     if non-nil, will be called before any other callbacks are invoked.")
  (receive [_ predicate false-value]
    "Returns a result-channel representing the first message in the queue or, if the queue
     is empty, the next message that is enqueued.")
  (cancel-receive [_ result-channel]
    "Cancels a receive operation. If the result-channel hasn't already been realized, it
     will be realized as a java.util.concurrent.CancellationException."))

;;;

(deftype ErrorQueue [error]
  QueueProtocol
  (error [_ _] false)
  (drained? [_] false)
  (ground [_] nil)
  (enqueue [_ _ _ _] false)
  (receive [_ _ _]
    (r/error-result error))
  (cancel-receive [_ _]
    false))

;;;

(defmacro consumption [consumer msg]
  `(let [consumer# ~consumer]
     (try
       (let [msg# ~msg
             predicate# (.predicate ^MessageConsumer consumer#)
             consumed?# (or (= nil predicate#) (predicate# msg#))]
         (Consumption.
           consumed?#
           false
           (if consumed?# msg# (.false-value ^MessageConsumer consumer#))
           (.result-channel ^MessageConsumer consumer#)))
       (catch Exception e#
         (Consumption.
           false
           true
           e#
           (.result-channel ^MessageConsumer consumer#))))))

(defmacro dispatch-consumption [consumption]
  `(let [c# ~consumption]
     (if (.error? ^Consumption c#)
       (r/error
         (.result-channel ^Consumption c#)
         (.result ^Consumption c#))
       (r/success
         (.result-channel ^Consumption c#)
         (.result ^Consumption c#)))))

(deftype MessageQueue
  [^AsymmetricLock lock
   ^ConcurrentLinkedQueue messages
   ^AtomicReference consumers
   closed?]

  QueueProtocol

  (error [_ error]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (doseq [^MessageConsumer c (seq (.get consumers))]
          (r/error (.result-channel c) error))
        (.set consumers nil)
        (.clear messages))))

  (drained? [_]
    (and closed? (= nil (.peek messages))))

  (ground [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (let [msgs (seq (.toArray messages))]
          (.clear messages)
          msgs))))

  (enqueue [_s msg persist? release-fn]
    (if closed?
      false
      (io! "Cannot modify non-transactional queues inside a transaction."
        (when-let [consumption-s
                   (l/with-non-exclusive-lock lock
                     (let [q ^ConcurrentLinkedQueue (.getAndSet consumers (ConcurrentLinkedQueue.))]

                       ;; check if there are any consumers
                       (if (.isEmpty q)
                         
                         ;; no consumers, just hold onto the message
                         (when persist?
                           (.offer messages (if (= nil msg) ::nil msg))
                           nil) 
                         
                         ;; handle the first consumer specially, since most of the time there will
                         ;; only be one
                         (let [c (consumption (.poll q) msg)]
                           (if (.isEmpty q)
                             
                             ;; there was only one
                             (if (.consumed? ^Consumption c)
                               c
                               (when persist?
                                 (.add messages (if (= nil msg) ::nil msg))
                                 nil))
                             
                             ;; iterate over the remaining consumers
                             (loop [cs (list c) consumed? false]
                               (if (.isEmpty q)
                                 (if consumed?
                                   cs
                                   (when persist?
                                     (.add messages (if (= nil msg) ::nil msg))
                                     nil))
                                 (let [consumer ^MessageConsumer (.poll q)
                                       c (consumption consumer msg)]
                                   (recur (cons c cs) (or consumed? (.consumed? ^Consumption c)))))))))))]
          (when release-fn
            (release-fn))
          (if (instance? Consumption consumption-s)
            (dispatch-consumption consumption-s)
            (doseq [c consumption-s]
              (dispatch-consumption c))))
        true)))

  (receive [_ predicate false-value]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (let [msg (.peek messages)]

          ;; check if there are any messages
          (if (= nil msg)

            ;; if there are no messages, add a consumer to the consumer queue
            (let [result-channel (r/result-channel)]
              (.add ^ConcurrentLinkedQueue (.get consumers)
                (MessageConsumer. predicate false-value result-channel))
              result-channel)

            ;; if there is a message, see if it satisfies the predicate
            (let [msg (if (= ::nil msg) nil msg)]
              (if (or (= nil predicate) (predicate msg))

                ;; it does, so consume the message and return it
                (do
                  (.poll messages)
                  (r/success-result msg))

                ;; it doesn't, so return the value indicating predicate failure
                (r/success-result false-value))))))))

  (cancel-receive [_ result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [removed?
            (l/with-exclusive-lock lock
              (let [q (.get consumers)]
                (loop [acc () remaining (seq q)]
                  (if (empty? remaining)
                    false
                    (if (= result-channel (.result-channel ^MessageConsumer (first remaining)))
                      (do
                        (.set consumers (ConcurrentLinkedQueue. (concat acc (rest remaining))))
                        true)
                      (recur (cons (first remaining) acc) (rest remaining)))))))]
        (if removed?
          (r/error result-channel (CancellationException.))
          false)))))

;;;

(defn queue [& messages]
  (MessageQueue.
    (l/asymmetric-lock false)
    (ConcurrentLinkedQueue. (map #(if (= nil %) ::nil %) messages))
    (AtomicReference. (ConcurrentLinkedQueue.))
    false))

(defn closed-queue [& messages]
  (MessageQueue.
    (l/asymmetric-lock false)
    (ConcurrentLinkedQueue. (map #(if (= nil %) ::nil %) messages))
    (AtomicReference. (ConcurrentLinkedQueue.))
    true))

(defn error-queue [error]
  (ErrorQueue. error))

(defn closed-copy [q]
  (apply closed-queue (ground q)))
