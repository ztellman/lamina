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
    [java.util.concurrent ConcurrentLinkedQueue]
    [java.util.concurrent.atomic AtomicReference]
    [java.util ArrayList]))

(deftype MessageConsumer [predicate false-value ^ResultChannel result-channel])

(deftype Consumption [consumed? result result-channel])

(defprotocol MessageQueue
  (close [_]) 
  (error [_ error]) 
  (drained? [_])
  (ground [_])
  (unground [_])
  (enqueue [_ msg])
  (receive [_ predicate false-value]) 
  (cancel-callback [_ result-channel]))

;;;

(deftype ErrorQueue [error]
  MessageQueue
  (close [_] false)
  (error [_ _] false)
  (drained? [_] false)
  (ground [_] nil)
  (unground [_] false)
  (enqueue [_ _] false)
  (receive [_ _ _]
    (r/error-result error))
  (cancel-callback [_ _]
    false))

(defmacro consumption [consumer msg]
  `(let [consumer# ~consumer
         msg# ~msg
         predicate# (.predicate ^MessageConsumer consumer#)
         consumed?# (or (= nil predicate#) (predicate# msg#))]
     (Consumption.
       consumed?#
       (if consumed?# msg# (.false-value ^MessageConsumer consumer#))
       (.result-channel ^MessageConsumer consumer#))))

(defmacro dispatch-consumption [consumption]
  `(let [c# ~consumption]
     (r/success
       (.result-channel ^Consumption c#)
       (.result ^Consumption c#))))

(deftype Queue
  [^AsymmetricLock lock
   ^ConcurrentLinkedQueue messages
   ^AtomicReference consumers
   ^:volatile-mutable closed?
   ^:volatile-mutable persist?]
  MessageQueue
  (close [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (set! closed? true)
        (= nil (.peek messages)))))
  
  (error [_ error]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (doseq [^MessageConsumer c (seq (.get consumers))]
          (r/error (.result-channel c) error))
        (.set consumers nil)
        (.clear messages))))

  (drained? [_]
    (l/with-non-exclusive-lock lock
      (and closed? (= nil (.peek messages)))))

  (ground [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (set! persist? false)
        (let [msgs (seq messages)]
          (.clear messages)
          msgs))))

  (unground [_]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (set! persist? true))))

  (enqueue [this msg]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (when-let [consumption-s
                 (l/with-non-exclusive-lock lock
                   (let [q ^ConcurrentLinkedQueue (.getAndSet consumers (ConcurrentLinkedQueue.))]
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
        (if (instance? Consumption consumption-s)
          (dispatch-consumption consumption-s)
          (doseq [c consumption-s]
            (dispatch-consumption c))))))

  (receive [this predicate false-value]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (let [msg (.peek messages)]
          (if (= nil msg)
            ;; if there are no messages, add a consumer to the consumer queue
            (let [result-channel (r/result-channel)]
              (.add ^ConcurrentLinkedQueue (.get consumers) (MessageConsumer. predicate false-value result-channel))
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

  (cancel-callback [_ result-channel]
    (io! "Cannot modify non-transactional queues inside a transaction."
      (l/with-exclusive-lock lock
        (let [q (.get consumers)]
          (loop [acc () remaining (seq q)]
            (if (empty? remaining)
              false
              (if (= result-channel (.result-channel ^MessageConsumer (first remaining)))
                (do
                  (.set consumers (ConcurrentLinkedQueue. (concat acc (rest remaining))))
                  true)
                (recur (cons (first remaining) acc) (rest remaining))))))))))

;;;

(defn queue [& messages]
  (Queue.
    (l/asymmetric-lock false)
    (ConcurrentLinkedQueue. (map #(if (= nil %) ::nil %) messages))
    (AtomicReference. (ConcurrentLinkedQueue.))
    false
    true))
