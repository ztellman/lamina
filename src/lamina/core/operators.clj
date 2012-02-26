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
    [java.util.concurrent
     ConcurrentLinkedQueue])) 

(set! *warn-on-reflection* true)

;;;

;; TODO: think about race conditions with closing the destination channel while a message is en-route
;; hand-over-hand locking in the node when ::consumed?

(deftype FinalValue [val])

(defmacro consume
  "something goes here"
  [ch & {:keys [map
                predicate
                initial-value
                reduce
                take-while
                final-messages
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

             ;; message predicate
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

             ;; general predicate
             predicate## ~predicate
             map## ~map
             reduce## ~reduce
             timeout## ~timeout]

         ;; if we're able to consume, take the unconsume function
         (if-let [unconsume# (n/consume
                               (emitter-node src-channel##)
                               (n/edge
                                 ~(or description "consume")
                                 (if dst##
                                   (receiver-node dst##)
                                   (n/terminal-node nil))))]

           ;; and define a more comprehensive cleanup function
           (let [cleanup#
                 (fn [val##]
                   ~@(when (and channel? final-messages)
                       `((let [msgs# (~final-messages val##)]
                           (doseq [msg# msgs#]
                             (enqueue dst## msg#)))))
                   (unconsume#)
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
                     ;; if we shouldn't even try to read a message, clean up
                     (if-not ~(if predicate
                                `(predicate## ~@(when reduce `(val##)))
                                true)
                       (cleanup# val##)

                       ;; if we should, call read-channel*
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

                         {:error-handler (fn [_#])}

                         (fn [msg##]
                           ;; if we didn't read a message, clean up
                           (if (identical? ::close msg##)
                             (cleanup# val##)

                             ;; update the reduce value
                             (p/run-pipeline ~(when reduce `(reduce## val## msg##))

                               {:error-handler (fn [_#])}

                               ;; once the value's realized, emit the next message
                               (fn [val##]
                                 (when dst##
                                   (enqueue dst##
                                     ~(if map
                                        `(map## ~@(when reduce `(val##)) msg##)
                                        `msg##)))
                                 val##)))))))

                   (fn [val#]
                     ;; if this isn't a terminal message from cleanup, restart
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

(defn receive-in-order
  "Consumes messages from the source channel, passing them to 'f' one at a time.  If 'f' returns
   a result-channel, consumption of the next message is deferred until it's realized.

   If an exception is thrown or the return result is realized as an error, the source channel is put
   into an error state."
  [ch f]
  (consume ch
    :channel nil
    :initial-value nil
    :reduce (fn [_ x]
              (p/run-pipeline
                {:error-handler #(error ch %)}
                (f x)))
    :description "receive-in-order"))

(defn last*
  "A dual to last.  Returns a result-channel that emits the final message from the source channel
   before it was closed.

   (last* (closed-channel 3 2 1)) => 1"
  [ch]
  (consume ch
    :channel nil
    :initial-value nil
    :reduce (fn [_ x] x)
    :description "last*"))

(defn take*
  "A dual to take.

   (take* 2 (channel 1 2 3)) => [1 2]"
  [n ch]
  (consume ch
    :description (str "take* " n)
    :initial-value 0
    :reduce (fn [n _] (inc n))
    :predicate #(< % n)))

(defn take-while*
  "A dual to take-while.

   (take-while* pos? (channel 1 2 0 4)) => [1 2]"
  [f ch]
  (consume ch
    :description "take-while*"
    :take-while f))

(defn reductions*
  "A dual to reductions.

   (reductions* max (channel 1 3 2)) => [1 3 3]"
  ([f ch]
     (let [ch* (mimic ch)]
       (consume ch
         :channel ch*
         :description "reductions*"
         :initial-value (p/run-pipeline (read-channel ch)
                          {:error-handler (fn [_])}
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
  "A dual to reduce.  Returns a result-channel that emits the final reduced value when the source
   channel has been drained.

   (reduce* max (channel 1 3 2)) => 3"
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
  [n step ch final-messages]
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
      :final-messages final-messages)))

(defn partition*
  "A dual to partition.

   (partition* 2 (channel 1 2 3)) => [[1 2]]"
  ([n ch]
     (partition* n n ch))
  ([n step ch]
     (partition- n step ch (constantly nil))))

(defn partition-all*
  "A dual to partition-all.

   (partition-all* 2 (closed-channel 1 2 3)) => [[1 2] [3]]"
  ([n ch]
     (partition-all* n n ch))
  ([n step ch]
     (partition- n step ch
       #(if (= n (count %))
          (partition-all n step (drop step %))
          (partition-all n step %)))))

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
  "Returns a sequence.  As elements of the sequence are realized, messages from the source
   channel are consumed.  If there are no messages are available to be consumed, execution will
   block until one is available.

   A 'timeout' can be defined, either as a number or a no-arg function that returns a number.  Each
   time the seq must wait for a message to consume, it will only wait that many milliseconds before
   giving up and ending the sequence."
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
  "An eager variant of lazy-channel-seq.  Blocks until the channel has been drained, or until 'timeout'
   milliseconds have elapsed."
  ([ch]
     (n/drain (emitter-node ch)))
  ([ch timeout]
     (let [start (System/currentTimeMillis)
           s (n/drain (emitter-node ch))]
       (doall
         (concat s
           (lazy-channel-seq ch
             #(max 0 (- timeout (- (System/currentTimeMillis) start)))))))))

;;;

(defn concat*
  "A dual to concat.

   (concat* (channel [1 2] [2 3])) => [1 2 3 4]"
  [ch]
  (let [ch* (mimic ch)]
    (bridge-join ch "concat*"
      #(doseq [msg %] (enqueue ch* msg))
      ch*)
    ch*))

(defn mapcat*
  "A dual to mapcat.

   (mapcat* reverse (channel [1 2] [3 4])) => [2 1 4 3]"
  [f ch]
  (->> ch (map* f) concat*))

(defn periodically
  "Returns a channel.  Every 'interval' milliseconds, 'f' is invoked with no arguments and the value is emitted
   as a message."
  [interval f]
  (let [ch (channel* :description (str "periodically " (describe-fn f)))]
    (p/run-pipeline (System/currentTimeMillis)

      ;; figure out how long to sleep, given the previous target timestamp
      (fn [timestamp]
        (let [target-timestamp (+ timestamp interval)]
          (r/timed-result
            (max 0.1 (- target-timestamp (System/currentTimeMillis)))
            target-timestamp)))

       ;; run the callback, and repeat
      (fn [timestamp]
        (let [result (enqueue ch (f))]
          (when-not (or (= :lamina/error! result)
                      (= :lamina/closed! result))
           (p/restart timestamp)))))
    ch))

(defn sample-every
  "Takes a source channel, and returns a channel that emits the most recent message from the source channel every
   'interval' milliseconds."
  [interval ch]
  (let [val (atom ::none)
        ch* (mimic ch)]
    (bridge-join ch (str "sample-every " interval)
      #(reset! val %)
      ch*)
    (siphon
      (->> #(deref val) (periodically interval) (remove* #(= ::none %)))
      ch*)
    ch*))

(defn partition-every
  "Takes a source channel, and returns a channel that repeatedly emits a collection of all messages from the source
   channel in the last 'interval' milliseconds."
  [interval ch]
  (let [q (ConcurrentLinkedQueue.)
        drain (fn []
                (loop [msgs []]
                  (if (.isEmpty q)
                    msgs
                    (recur (conj msgs (.remove q))))))
        ch* (mimic ch)]
    (bridge-join ch (str "partition-every " interval)
      #(.add q %)
      ch*)
    (siphon (periodically interval drain) ch*)
    ch*))



