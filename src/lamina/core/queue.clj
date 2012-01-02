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

(deftype MessageConsumer
  [predicate
   false-value
   ^boolean claim?
   ^ResultChannel result-channel]
  Object
  (equals [_ x]
    (and
      (instance? MessageConsumer x)
      (= result-channel (.result-channel ^MessageConsumer x))))
  (hashCode [_]
    (hash result-channel)))

(deftype Consumption
  [type ;; ::consumed, ::not-consumed, ::error, ::no-dispatch
   ^boolean claimed?
   result
   ^ResultChannel result-channel])

(defn no-consumption [result-channel]
  (Consumption. ::no-dispatch false nil result-channel))

(defn claimed-consumption [value result-channel]
  (Consumption. ::consumed true value result-channel))

(defn failed-claimed-consumption [false-value result-channel]
  (Consumption. ::not-consumed true false-value result-channel))

(defn error-consumption [claimed? error result-channel]
  (Consumption. ::error claimed? error result-channel))

(defmacro consumption [consumer msg]
  `(let [^MessageConsumer consumer# ~consumer]
     (try
       (let [msg# ~msg
             predicate# (.predicate consumer#)
             consume?# (or (= nil predicate#) (predicate# msg#))]
         (if (or (not (.claim? consumer#)) (r/claim (.result-channel consumer#)))

           ;; either we didn't need to claim the result-channel, or we succeeded
           (Consumption.
             (if consume?# ::consumed ::not-consumed)
             (.claim? consumer#)
             (if consume?# msg# (.false-value consumer#))
             (.result-channel consumer#))

           ;; we failed to claim it
           (no-consumption (.result-channel consumer#))))
       
       (catch Exception e#
         (if (or (not (.claim? consumer#)) (r/claim (.result-channel consumer#)))
           (error-consumption (.claim? consumer#) e# (.result-channel consumer#))
           (no-consumption (.result-channel consumer#)))))))

(defmacro dispatch-consumption [consumption]
  `(let [^Consumption c# ~consumption]
     (case (.type c#)
       (::not-consumed ::consumed)
       (if (.claimed? c#)
         (r/success! (.result-channel c#) (.result c#))
         (r/success (.result-channel c#) (.result c#)))

       ::error
       (if (.claimed? c#)
         (r/error! (.result-channel c#) (.result c#))
         (r/error (.result-channel c#) (.result c#)))

       nil)))

;;;

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
  (receive [_ predicate false-value result-channel]
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
  (receive [_ _ _ result-channel]
    (if result-channel
      (do
        (when (r/claim result-channel)
          (r/error! result-channel error))
        result-channel)
      (r/error-result error)))
  (cancel-receive [_ _]
    false))

(deftype DrainedQueue []
  QueueProtocol
  (error [_ _] false)
  (drained? [_] true)
  (ground [_] nil)
  (enqueue [_ _ _ _] false)
  (receive [_ _ _ result-channel]
    (if result-channel
      (do
        (when (r/claim result-channel)
          (r/error! result-channel :lamina/drained!))
        result-channel)
      (r/error-result :lamina/drained!)))
  (cancel-receive [_ _] false))

;;;

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
          (map #(if (= ::nil %) nil %) msgs)))))

  (enqueue [_ msg persist? release-fn]
    (if closed?
      false
      (io! "Cannot modify non-transactional queues inside a transaction."
        (when-let [x
                   (l/with-lock lock
                     (let [q ^ConcurrentLinkedQueue (.getAndSet consumers (ConcurrentLinkedQueue.))]

                       ;; check if there are any consumers
                       (if (.isEmpty q)
                         
                         ;; no consumers, just hold onto the message
                         (when persist?
                           (.offer messages (if (= nil msg) ::nil msg))
                           :lamina/enqueued) 
                         
                         ;; handle the first consumer specially, since most of the time there will
                         ;; only be one
                         (let [^Consumption c (consumption (.poll q) msg)]
                           (if (.isEmpty q)
                             
                             ;; there was only one
                             (if (= ::consumed (.type c))
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
                                 (let [^MessageConsumer consumer (.poll q)
                                       ^Consumption c (consumption consumer msg)]
                                   (recur (cons c cs) (or consumed? (= ::consumed (.type c))))))))))))]
          (when release-fn
            (release-fn))
          (cond
            (= :lamina/enqueued x)
            x

            (instance? Consumption x)
            (dispatch-consumption x)

            :else
            (do
              (doseq [c x]
                (dispatch-consumption c))
              :lamina/queue-split))))))

  (receive [_ predicate false-value result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [consumption
            (l/with-exclusive-lock lock
              (let [msg (.peek messages)]
            
                ;; check if there are any messages
                (if (= nil msg)
              
                  ;; if there are no messages, add a consumer to the consumer queue
                  (let [rc (or result-channel (r/result-channel))]
                    (.add ^ConcurrentLinkedQueue (.get consumers)
                      (MessageConsumer. predicate false-value (boolean result-channel) rc))
                    (no-consumption rc))
              
                  (let [msg (if (= ::nil msg) nil msg)]
                    (if result-channel

                      ;; a result-channel has been provided for us
                      (if (or (= nil predicate) (predicate msg))

                        ;; if the predicate succeeds, see if we can claim the channel
                        (if (r/claim result-channel)

                          ;; if so, success!
                          (do
                            (.poll messages)
                            (claimed-consumption msg result-channel))

                          ;; otherwise, return the channel as is
                          (no-consumption result-channel))

                        ;; the predicate failed
                        (if (r/claim result-channel)
                          (failed-claimed-consumption false-value result-channel)
                          (no-consumption result-channel)))

                      (if (or (= nil predicate) (predicate msg))
                        
                        ;; it does, so consume the message and return it
                        (do
                          (.poll messages)
                          (no-consumption (r/success-result msg)))
                        
                        ;; it doesn't, so return the value indicating predicate failure
                        (no-consumption (r/success-result false-value))))))))]
        (dispatch-consumption consumption)
        (.result-channel ^Consumption consumption))))

  (cancel-receive [_ result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [removed?
            (l/with-exclusive-lock lock
              (.remove ^ConcurrentLinkedQueue
                (.get consumers)
                (MessageConsumer. nil nil false result-channel)))]
        (if removed?
          (do
            (when (r/claim result-channel)
              (r/error! result-channel :lamina/cancelled))
            true) 
          false)))))

;;;

(defn queue [& messages]
  (MessageQueue.
    (l/asymmetric-lock)
    (ConcurrentLinkedQueue. (map #(if (= nil %) ::nil %) messages))
    (AtomicReference. (ConcurrentLinkedQueue.))
    false))

(defn closed-queue [& messages]
  (MessageQueue.
    (l/asymmetric-lock)
    (ConcurrentLinkedQueue. (map #(if (= nil %) ::nil %) messages))
    (AtomicReference. (ConcurrentLinkedQueue.))
    true))

(defn error-queue [error]
  (ErrorQueue. error))

(defn drained-queue []
  (DrainedQueue.))

(defn closed-copy [q]
  (apply closed-queue (ground q)))
