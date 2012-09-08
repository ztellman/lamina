;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.queue
  (:use
    [potemkin])
  (:require
    [lamina.core.result :as r]
    [lamina.core.lock :as l]
    [clojure.tools.logging :as log])
  (:import
    [clojure.lang PersistentQueue]
    [lamina.core.lock AsymmetricLock]
    [lamina.core.result ResultChannel]
    [java.util.concurrent ConcurrentLinkedQueue]))

(set! *warn-on-reflection* true)

(deftype-once MessageConsumer
  [predicate
   false-value
   ^ResultChannel result-channel])

(deftype-once SimpleConsumer
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

(defrecord-once Consumption
  [type ;; ::consumed, ::not-consumed, ::error, ::no-dispatch
   result
   ^ResultChannel result-channel])

(deftype-once Consumptions [s])

(deftype-once FailedConsumptions [s])

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
(defprotocol-once IEventQueue
  (error [_ error]
    "All pending receives are resolved as errors. It's expected that the queue will
     be swapped out for an error-emitting queue at this point.")
  (close [_]
    )
  (closed? [_]
    )
  (drained? [_]
    "Returns true if the queue is closed and empty.")
  (drain [_]
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
    "Cancels a receive operation."))

;;;

(deftype-once ErrorQueue [error]
  clojure.lang.Counted
  (count [_] 0)
  IEventQueue
  (error [_ _] false)
  (close [_] false)
  (drained? [_] false)
  (closed? [_] false)
  (drain [_] nil)
  (messages [_] nil)
  (enqueue [_ _ _ _] false)
  (receive [this]
    (receive this nil nil nil))
  (receive [_ _ _ result-channel]
    (if result-channel
      (do
        (when (r/claim result-channel)
          (r/error! result-channel error))
        result-channel)
      (r/error-result error)))
  (cancel-receive [_ _]
    false))

(deftype-once DrainedQueue []
  clojure.lang.Counted
  (count [_] 0)
  IEventQueue
  (error [_ _] false)
  (close [_] false)
  (drained? [_] true)
  (closed? [_] true)
  (drain [_] nil)
  (messages [_] nil)
  (enqueue [_ _ _ _] false)
  (receive [this]
    (receive this nil nil nil))
  (receive [_ _ _ result-channel]
    (if result-channel
      (do
        (when (r/claim result-channel)
          (r/error! result-channel :lamina/drained!))
        result-channel)
      (r/error-result :lamina/drained!)))
  (cancel-receive [_ _] false))

;;;

(deftype-once EventQueue
  [^AsymmetricLock lock
   ^ConcurrentLinkedQueue messages
   ^ConcurrentLinkedQueue consumers
   ^{:volatile-mutable true} closed?]

  clojure.lang.Counted

  (count [_]
    (.size messages))

  IEventQueue

  ;;
  (error [_ error]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [consumers
            (l/with-exclusive-lock lock
              (let [cs (.toArray consumers)]
                (.clear consumers)
                (.clear messages)))]
        (doseq [^MessageConsumer c consumers]
          (r/error (.result-channel c) error)))))

  ;; this isn't super atomic (multiple calls can receive 'true', though), but we
  ;; don't much care 
  (close [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (if closed?
        false
        (let [consumers
              (l/with-exclusive-lock lock
                (let [cs (.toArray consumers)]
                  (.clear consumers)
                  cs))]
          (set! closed? true)
          (doseq [c consumers]
            (if (instance? MessageConsumer c)
              (r/error (.result-channel ^MessageConsumer c) :lamina/drained!)
              (r/error (.result-channel ^SimpleConsumer c) :lamina/drained!)))
          true))))

  ;;
  (drained? [_]
    (and closed? (identical? nil (.peek messages))))

  ;;
  (closed? [_]
    closed?)

  ;;
  (drain [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (when-not (.isEmpty messages)
          (let [msgs (seq (.toArray messages))]
            (.clear messages)
            (map #(if (identical? ::nil %) nil %) msgs))))))

  ;;
  (messages [_]
    (map #(if (identical? ::nil %) nil %) (seq (.toArray messages))))

  ;;
  (append [_ msgs]
    (.addAll messages (map #(if (identical? nil %) ::nil %) msgs)))

  ;;
  (enqueue [_ msg persist? release-fn]
    (if closed?
      (do
        (when release-fn
          (release-fn))
        false)
      (io! "Cannot modify non-transactional queues inside a transaction."
        (let [x (l/with-exclusive-lock lock
                  (when release-fn
                    (release-fn))

                  ;; check if there are any consumers
                  (if (.isEmpty consumers)
                      
                    ;; no consumers, just hold onto the message
                    (if persist?
                      (do
                        (.add messages (if (nil? msg) ::nil msg))
                        :lamina/enqueued)
                      :lamina/discarded) 
                      
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
                                (.add messages (if (nil? msg) ::nil msg)))
                              (FailedConsumptions. cs))
                            (let [^Consumption c (consumption (.poll consumers) msg)]
                              (if (consumed? c)
                                (Consumptions. (cons c cs))
                                (recur (cons c cs))))))))))]

          (cond
            (identical? :lamina/enqueued x)
            x

            (identical? :lamina/discarded x)
            x

            (instance? Consumption x)
            (dispatch-consumption x)

            (instance? Consumptions x)
            (do
              (doseq [c (.s ^Consumptions x)]
                (dispatch-consumption c))
              :lamina/queue-split)

            :else
            (do
              (doseq [c (.s ^FailedConsumptions x)]
                (dispatch-consumption c))
              (if persist?
                :lamina/enqueued
                :lamina/discarded)))))))

  ;;
  (receive [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (if-let [msg (.poll messages)]

          (r/success-result (if (identical? ::nil msg) nil msg))
              
          (if closed?
            (r/error-result :lamina/drained!)
            (let [result-channel (r/result-channel)]
              (.add consumers (SimpleConsumer. result-channel))
              result-channel))))))
  
  (receive [_ predicate false-value result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (let [^Consumption consumption
            (l/with-exclusive-lock lock

              (let [msg (.peek messages)]
            
                ;; check if there are any messages
                (if (identical? nil msg)
              
                  ;; if there are no messages, add a consumer to the consumer queue
                  (if closed?
                    (error-consumption :lamina/drained! result-channel)
                    (let [rc (or result-channel (r/result-channel))]
                      (.add consumers (MessageConsumer.
                                        predicate
                                        false-value
                                        rc))
                      (no-consumption rc)))
              
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

(defn poll-queue [q]
  (let [x (first (ensure q))]
    (alter q pop)
    x))

(defn persistent-queue
  ([]
     PersistentQueue/EMPTY)
  ([s]
     (if (empty? s)
       PersistentQueue/EMPTY
       (apply conj PersistentQueue/EMPTY s))))

(defmacro dosync-yielding [& body]
  `(dosync
     (try
       ~@body
       (catch Error e#
         (Thread/sleep 1)
         (throw e#)))))

(deftype-once TransactionalEventQueue
  [messages
   consumers
   closed?]

  clojure.lang.Counted

  (count [_]
    (count @messages))

  IEventQueue

  ;;
  (error [_ error]
    (let [consumers (dosync
                      (let [cs (ensure consumers)]
                        (ref-set consumers nil)
                        (ref-set messages nil)
                        cs))]
      (doseq [^MessageConsumer c consumers]
        (r/error (.result-channel c) error))))

  ;;
  (close [this]
    (if @closed?
      false
      (let [cs (dosync
                 (let [cs (ensure consumers)]
                   (ref-set closed? true)
                   (ref-set consumers nil)
                   cs))]
        (doseq [c cs]
          (if (instance? MessageConsumer c)
            (r/error (.result-channel ^MessageConsumer c) :lamina/drained!)
            (r/error (.result-channel ^SimpleConsumer c) :lamina/drained!)))
        true)))

  ;;
  (drained? [_]
    (and @closed? (empty? @messages)))

  ;;
  (closed? [_]
    @closed?)

  ;;
  (drain [_]
    (dosync
      (let [msgs (ensure messages)]
        (ref-set messages PersistentQueue/EMPTY)
        (seq msgs))))

  ;;
  (messages [_]
    (seq @messages))

  ;;
  (append [_ msgs]
    (when-not (empty? msgs)
      (dosync
        (apply alter messages conj msgs))))

  ;;
  (enqueue [_ msg persist? release-fn]
    (let [release-once (delay (when release-fn (release-fn)))
          x (dosync
              @release-once
              (if (ensure closed?)
                :lamina/already-closed!
                (let [cs (ensure consumers)]
                      
                  (if (empty? cs)
                        
                    ;; no consumers, just hold onto the message
                    (when persist?
                      (alter messages conj msg)
                      :lamina/enqueued) 
                        
                    ;; handle the first consumer specially, since most of the time
                    ;; there will only be one
                    (let [^Consumption c (consumption (poll-queue consumers) msg)]
                      (if (consumed? c)
                        c
                            
                        ;; iterate over the remaining consumers
                        (loop [cs (list c)]
                          (if (empty? @consumers)
                            (do
                              (when persist?
                                (alter messages conj msg))
                              cs)
                            (let [^Consumption c (consumption (poll-queue consumers) msg)]
                              (if (consumed? c)
                                (cons c cs)
                                (recur (cons c cs))))))))))))]

      (cond
        (keyword? x)
        x
          
        (instance? Consumption x)
        (dispatch-consumption x)
          
        :else
        (do
          (doseq [c x]
            (dispatch-consumption c))
          :lamina/queue-branch))))

  ;;
  (receive [_]
    (dosync-yielding
      (if-not (empty? (ensure messages))

        (r/success-result (poll-queue messages))
          
        (if (ensure closed?)
          (r/error-result :lamina/drained!)
          (let [result-channel (r/result-channel)]
            (alter consumers conj (SimpleConsumer. result-channel))
            result-channel)))))
  
  (receive [this predicate false-value result-channel]
    (let [^Consumption consumption
          (dosync-yielding
            (if (empty? (ensure messages))
            
              (if (ensure closed?)
                (error-consumption :lamina/drained! result-channel)
                (let [rc (or result-channel (r/result-channel))]
                  (alter consumers conj
                    (MessageConsumer.
                      predicate
                      false-value
                      rc))
                  (no-consumption rc)))
                
              (let [msg (first @messages)
                    c (consumption
                        (MessageConsumer.
                          predicate
                          false-value
                          result-channel)
                        msg)]
                (when (consumed? c)
                  (alter messages pop))
                c)))]
      (let [result (dispatch-consumption consumption)]
        (if-not (.result-channel consumption)
          result
          (.result-channel consumption)))))

  ;; TODO: make this a bit more efficient
  (cancel-receive [_ result-channel]
    (let [removed?
          (dosync
            (let [c (SimpleConsumer. result-channel)
                  cs (remove #(.equals c %) (ensure consumers))]
              (if (not= (count cs) (count @consumers))
                (do
                  (ref-set consumers (persistent-queue cs))
                  true)
                false)))]
      (if removed?
        (do
          (when (r/claim result-channel)
            (r/error! result-channel :lamina/cancelled))
          true) 
        false))))

;;;

(defn queue
  ([]
     (queue nil))
  ([messages]
     (EventQueue.
       (l/lock)
       (if messages
         (ConcurrentLinkedQueue. (map #(if (identical? nil %) ::nil %) messages))
         (ConcurrentLinkedQueue.))
       (ConcurrentLinkedQueue.)
       false)))

(defn transactional-queue
  ([]
     (transactional-queue nil))
  ([messages]
     (TransactionalEventQueue.
       (ref (persistent-queue messages))
       (ref (persistent-queue))
       (ref false))))

(defn error-queue [error]
  (ErrorQueue. error))

(defn drained-queue []
  (DrainedQueue.))

(defn transactional-copy [q]
  (cond
    (nil? q) nil
    (drained? q) (drained-queue)
    (instance? TransactionalEventQueue q) q
    :else (let [^EventQueue q q
                msgs (.messages q)
                consumers (.consumers q)]
            (TransactionalEventQueue.
              (ref (persistent-queue msgs))
              (ref (persistent-queue consumers))
              (ref (closed? q))))))
