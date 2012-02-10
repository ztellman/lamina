;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.operators
  (:use
    [potemkin :only (unify-gensyms)]
    [lamina.core channel utils])
  (:require
    [lamina.core.node :as n]
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

(deftype FinalValue [val])

(defmacro consume
  "Consumes messages one-by-one from the source channel, per the read-channel* style parameters.
   Valid options are:

   :message-predicate - a function that takes a message, and returns true if it should be consumed
   :predicate - a no-arg function that returns true if another message should be read
   :message-transform -
   :reduce -
   :initial-value -
   :timeout - a no-arg function that returns the maximum time that should be spent waiting for the next message
   :description - a description of the consumption mechanism
   :channel - the destination channel for the messages

   If the predicate returns false or the timeout elapses, the consumption will cease."
  [ch & {:keys [map
                predicate
                initial-value
                reduce
                take-while
                final-message
                timeout
                description
                channel] :as m}]
  (let [channel? (not (and (contains? m :channel) (nil? channel)))
        take-while? (or channel? take-while)]
    (unify-gensyms
      `(let [src-channel## ~ch
             dst## ~(when channel?
                      (or channel `(mimic src-channel##)))
             dst-node## (when dst## (receiver-node dst##)) 
             initial-val# ~initial-value
             take-while## ~take-while
             take-while## ~(if take-while
                             `(fn [~@(when reduce `(val##)) x#]
                                (and
                                  (or
                                    (nil? dst-node##)
                                    (not (n/closed? dst-node##)))
                                  (take-while##
                                    ~@(when reduce `(val##))
                                    x#)))
                             `(fn [~'& _#]
                                (or
                                  (nil? dst-node##)
                                  (not (n/closed? dst-node##)))))
             predicate## ~predicate
             map## ~map
             reduce## ~reduce
             timeout## ~timeout]
         (if-let [unconsume# (n/consume
                               (emitter-node src-channel##)
                               (n/edge
                                 ~(or description "consume")
                                 (if dst##
                                   (receiver-node dst##)
                                   (n/terminal-node nil))))]

           (let [cleanup#
                 (fn [val##]
                   (unconsume#)
                   ~@(when (and channel? final-message)
                       `((let [msg# (~final-message val##)]
                           (when-not (nil? msg#)
                             (enqueue dst## msg#)))))
                   (when dst## (close dst##))
                   (FinalValue. val##))

                 result#
                 (p/run-pipeline initial-val#
                   {:error-handler (fn [ex#]
                                     (log/error ex# "error in consume")
                                     (if dst##
                                       (error dst## ex#)
                                       (p/redirect (p/pipeline (constantly (r/error-result ex#))) nil)))}
                   (fn [val##]
                     (if-not ~(if predicate
                                `(predicate## ~@(when reduce `(val##)))
                                true)
                       (cleanup# val##)
                       (p/run-pipeline
                         (read-channel* src-channel##
                           :on-false ::close
                           :on-timeout ::close
                           :on-drained ::close
                           ~@(when timeout
                               `(:timeout (timeout##)))
                           ~@(when take-while?
                               `(:predicate
                                  ~(if reduce
                                     `(fn [msg#]
                                        (take-while## ~@(when reduce `(val##)) msg#))
                                     `take-while##))))
                         (fn [msg##]
                           (if (identical? ::close msg##)
                             (cleanup# val##)
                             (let [val## ~(when reduce `(reduce## val## msg##))]
                               (when dst##
                                 (enqueue dst##
                                   ~(if map
                                      `(map## ~@(when reduce `(val##)) msg##)
                                      `msg##)))
                               val##))))))
                   (fn [val#]
                     (if (instance? FinalValue val#)
                       (.val ^FinalValue val#)
                       (p/restart val#))))]
             (if dst##
               dst##
               result#))

           ;; something's already attached to the source
           (if dst##
             (do
               (error dst## :lamina/already-consumed!)
               dst##)
             (r/error-result :lamina/already-consumed!)))))))

;;;

(defn last* [ch]
  (consume ch
    :channel nil
    :initial-value nil
    :reduce (fn [_ x] x)
    :description "last*"))

(defn take* [n ch]
  (consume ch
    :description (str "take* " n)
    :initial-value 0
    :reduce (fn [n _] (inc n))
    :predicate #(< % n)))

(defn take-while* [f ch]
  (consume ch
    :description "take-while*"
    :take-while f))

(defn reductions*
  ([f ch]
     (let [ch* (mimic ch)]
       (consume ch
         :channel ch*
         :description "reductions*"
         :initial-value (p/run-pipeline (read-channel ch)
                          #(do (enqueue ch* %) %))
         :reduce f
         :map (fn [v _] v))))
  ([f val ch]
     (consume ch
       :channel (let [ch* (mimic ch)]
                  (enqueue ch* val)
                  ch*)
       :description "reductions*"
       :initial-value val
       :reduce f
       :map (fn [v _] v))))

(defn reduce*
  ([f ch]
     (consume ch
       :channel nil
       :description "reduce*"
       :initial-value (read-channel ch)
       :reduce f
       :map (fn [v _] v)))
  ([f val ch]
     (consume ch
       :channel nil
       :description "reduce*"
       :initial-value val
       :reduce f
       :map (fn [v _] v))))

(defn partition-
  [n step ch final-message]
  (remove* nil?
    (consume ch
      :description "partition*"
      :initial-value []
      :reduce (fn [v msg]
                (if (= n (count v))
                  (-> (drop step v) vec (conj msg))
                  (conj v msg)))
      :map (fn [v _]
             (when (= n (count v))
               v))
      :final-message final-message)))

(defn partition*
  ([n ch]
     (partition* n n ch))
  ([n step ch]
     (partition- n step ch (constantly nil))))

(defn partition-all*
  ([n ch]
     (partition-all* n n ch))
  ([n step ch]
     (partition- n step ch #(when-not (empty? %) %))))

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
     (lazy-channel-seq ch nil))
  ([ch timeout]
     (let [timeout-fn (when timeout
                        (if (number? timeout)
                          (constantly timeout)
                          timeout))
           e (n/edge "lazy-channel-seq" (n/terminal-node nil))]
       (if-let [unconsume (n/consume (emitter-node ch) e)]
         (lazy-channel-seq-
           (if timeout-fn
             #(read-channel* ch :timeout (timeout-fn) :on-timeout ::end :on-drained ::end)
             #(read-channel* ch :on-drained ::end))
           unconsume)
         (throw (IllegalStateException. "Can't consume, channel already in use."))))))

(defn channel-seq
  ([ch]
     (n/ground (emitter-node ch)))
  ([ch timeout]
     (let [start (System/currentTimeMillis)
           s (n/ground (emitter-node ch))]
       (concat s
         (lazy-channel-seq ch
           #(max 0 (- timeout (- (System/currentTimeMillis) start))))))))
