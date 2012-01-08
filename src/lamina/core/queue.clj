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
   ^ResultChannel result-channel])

(deftype SimpleConsumer
  [^ResultChannel result-channel]
  Object
  (equals [_ x]
    (or
      (and
        (instance? SimpleConsumer x)
        (identical? result-channel (.result-channel ^SimpleConsumer x)))
      (and
        (instance? MessageConsumer x)
        (identical? result-channel (.result-channel ^MessageConsumer x)))))
  (hashCode [_]
    (hash result-channel)))

(defrecord Consumption
  [type ;; ::consumed, ::not-consumed, ::error, ::no-dispatch
   result
   ^ResultChannel result-channel])

(defn no-consumption [result-channel]
  (Consumption. ::no-dispatch nil result-channel))

(defn error-consumption [error result-channel]
  (Consumption. ::error error result-channel))

(defn consumption [consumer msg]
  (let [msg# msg
         consumer# consumer]
     (if (instance? SimpleConsumer consumer#)
       (let [result-channel# (.result-channel ^SimpleConsumer consumer#)]
         (Consumption.
           (if (or (identical? nil result-channel#) (r/claim result-channel#))
             ::consumed
             ::not-consumed)
           msg#
           result-channel#))
       (let [^MessageConsumer consumer# consumer#
             predicate# (.predicate consumer#)
             result-channel# (.result-channel consumer#)]
         (try
           (let [consume?# (or (identical? nil predicate#) (predicate# msg#))]
             (if (or (identical? nil result-channel#) (r/claim result-channel#))
               
               ;; either we didn't need to claim the result-channel, or we succeeded
               (Consumption.
                 (if consume?# ::consumed ::not-consumed)
                 (if consume?# msg# (.false-value consumer#))
                 result-channel#)
               
               ;; we failed to claim it
               (no-consumption result-channel#)))
           
           (catch Exception e#
             (if (or (identical? nil result-channel#) (r/claim (.result-channel consumer#)))
               (error-consumption e# result-channel#)
               (no-consumption result-channel#))))))))

(defmacro dispatch-consumption [consumption]
  `(let [^Consumption c# ~consumption
         result-channel# (.result-channel c#)]
     (case (.type c#)
       (::not-consumed ::consumed)
       (if (identical? nil result-channel#)
         (r/success-result (.result c#))
         (r/success! result-channel# (.result c#)))

       ::error
       (if (identical? nil result-channel#)
         (r/error-result (.result c#))
         (r/error! result-channel# (.result c#)))

       nil)))

(defmacro consumed? [consumption]
  `(let [consumption# ~consumption]
     (identical? ::consumed (.type ^Consumption consumption#))))

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
  (messages [_]
    "Returns all messages currently in the queue.")
  (append [_ msgs]
    "Batch appending of messages to the queue. Should only be invoked where there's guaranteed
     to be no consumers.")
  (enqueue [_ msg persist? release-fn]
    "Enqueues a message into the queue. If 'persist?' is false, the message will only
     be sent to pending receivers, and not remain in the queue. The 'release-fn' callback,
     if non-nil, will be called before any other callbacks are invoked.")
  (receive [_] [_ predicate false-value result-channel]
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
  (messages [_] nil)
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
  (messages [_] nil)
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
   ^ConcurrentLinkedQueue consumers
   closed?]

  QueueProtocol

  ;;
  (error [_ error]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (doseq [^MessageConsumer c (seq consumers)]
          (r/error (.result-channel c) error))
        (.clear consumers)
        (.clear messages))))

  ;;
  (drained? [_]
    (and closed? (identical? nil (.peek messages))))

  ;;
  (ground [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (when-not (.isEmpty messages)
          (let [msgs (seq (.toArray messages))]
            (.clear messages)
            (map #(if (identical? ::nil %) nil %) msgs))))))

  ;;
  (messages [_]
    (seq (.toArray messages)))

  ;;
  (append [_ msgs]
    (.addAll messages (map #(if (identical? nil %) ::nil %) msgs)))

  ;;
  (enqueue [_ msg persist? release-fn]
    (if closed?
      false
      (io! "Cannot modify non-transactional queues inside a transaction."
        (let [msg (if (identical? nil msg) ::nil msg)
              x (l/with-exclusive-lock lock

                  ;; check if there are any consumers
                  (if (.isEmpty consumers)
                    
                    ;; no consumers, just hold onto the message
                    (when persist?
                      (.add messages msg)
                      :lamina/enqueued) 
                    
                    ;; handle the first consumer specially, since most of the time
                    ;; there will only be one
                    (let [^Consumption c (consumption (.poll consumers) msg)]
                      (if (consumed? c)
                        c
                        
                        ;; iterate over the remaining consumers
                        (loop [cs (list c)]
                          (if (.isEmpty consumers)
                            (do
                              (when persist?
                                (.add messages msg))
                              cs)
                            (let [^Consumption c (consumption (.poll consumers) msg)]
                              (if (consumed? c)
                                (cons c cs)
                                (recur (cons c cs))))))))))]
          (when release-fn
            (release-fn))

          (cond
            (identical? :lamina/enqueued x)
            x

            (instance? Consumption x)
            (dispatch-consumption x)

            :else
            (do
              (doseq [c x]
                (dispatch-consumption c))
              :lamina/queue-split))))))

  ;;
  (receive [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (if-let [msg (.poll messages)]

          (r/success-result (if (identical? ::nil msg) nil msg))
              
          ;; if there are no messages, add a consumer to the consumer queue
          (let [result-channel (r/result-channel)]
            (.add consumers (SimpleConsumer. result-channel))
            result-channel)))))
  
  (receive [_ predicate false-value result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [^Consumption consumption
            (l/with-exclusive-lock lock
              (let [msg (.peek messages)]
            
                ;; check if there are any messages
                (if (identical? nil msg)
              
                  ;; if there are no messages, add a consumer to the consumer queue
                  (let [rc (or result-channel (r/result-channel))]
                    (.add consumers (MessageConsumer.
                                      predicate
                                      false-value
                                      rc))
                    (no-consumption rc))
              
                  (let [msg (if (identical? ::nil msg) nil msg)
                        c (consumption
                            (MessageConsumer.
                              predicate
                              false-value
                              result-channel)
                            msg)]
                    (when (consumed? c)
                      (.poll messages))
                    c))))]
        (let [result (dispatch-consumption consumption)]
          (if-not (.result-channel consumption)
            result
            (.result-channel consumption))))))

  ;;
  (cancel-receive [_ result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [removed?
            (l/with-exclusive-lock lock
              (.remove consumers (SimpleConsumer. result-channel)))]
        (if removed?
          (do
            (when (r/claim result-channel)
              (r/error! result-channel :lamina/cancelled))
            true) 
          false)))))

;;;

(defn queue
  ([]
     (queue nil))
  ([messages]
     (MessageQueue.
       (l/lock)
       (if messages
         (ConcurrentLinkedQueue. (map #(if (identical? nil %) ::nil %) messages))
         (ConcurrentLinkedQueue.))
       (ConcurrentLinkedQueue.)
       false)))

(defn closed-queue
  ([]
     (closed-queue nil))
  ([messages]
     (MessageQueue.
       (l/lock)
       (if messages
         (ConcurrentLinkedQueue. (map #(if (identical? nil %) ::nil %) messages))
         (ConcurrentLinkedQueue.))
       (ConcurrentLinkedQueue.)
       true)))

(defn error-queue [error]
  (ErrorQueue. error))

(defn drained-queue []
  (DrainedQueue.))

(defn closed-copy [q]
  (if q
    (closed-queue (ground q))
    (drained-queue)))
