;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.operators
  (:use
    [potemkin]
    [lamina.core utils channel threads])
  (:require
    [lamina.executor.core :as ex]
    [lamina.core.graph :as g]
    [lamina.core.lock :as l]
    [lamina.core.result :as r]
    [lamina.time :as t]
    [lamina.core.pipeline :as p]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.lock
     Lock]
    [java.util
     BitSet]
    [java.util.concurrent
     ConcurrentLinkedQueue]
    [java.util.concurrent.atomic
     AtomicReferenceArray
     AtomicReference
     AtomicLong]
    [java.math
     BigInteger])) 

;;;

(defn bridge-in-order
  "something goes here"
  [src dst description &
   {:keys [predicate
           callback
           on-complete
           wait-on-callback?]}]

  (p/run-pipeline (g/consume
                    (emitter-node src)
                    (g/edge
                      (when dst description)
                      (if dst
                        (receiver-node dst)
                        (g/terminal-propagator description))))

    (fn [unconsume]

      (if-not unconsume

        ;; something's already consuming the channel
        (do
          (when dst
            (error dst :lamina/already-consumed! false))
          (r/error-result :lamina/already-consumed!))
        
        (let [cleanup (fn cleanup []
                        (r/defer-within-transaction (cleanup)
                          (unconsume)
                          (when on-complete (on-complete))
                          (when dst (close dst))))]
          
          (p/run-pipeline nil
            
            {:error-handler (fn [ex] (if dst
                                       (error dst ex false)
                                       (error src ex false)))
             :finally cleanup}
            
            (fn [_]
              (read-channel* src
                :on-drained ::stop
                :predicate predicate
                :on-false ::stop))
            
            (fn [msg]
              (if (identical? ::stop msg)
                (p/complete nil)
                (when callback
                  (let [f #(let [result (callback msg)]
                             (when wait-on-callback?
                               result))]
                    (r/defer-within-transaction (f)
                      (f))))))
            
            (fn [_]
              (if dst
                (if-not (closed? dst)
                  (p/restart))
                (p/restart)))))))))

(defn last*
  "A dual to last."
  [ch]
  (let [r (r/result-channel)
        msg (atom nil)]
    (p/run-pipeline
      (bridge-in-order ch nil "last*"
        :callback #(reset! msg %))
      (fn [_] @msg))))

(defn receive-in-order
  "Consumes messages from the source channel, passing them to 'f' one at a time.  If
   'f' returns a result-channel, consumption of the next message is deferred until
   it's realized.

   If an exception is thrown or the return result is realized as an error, the source
   channel is put into an error state."
  [ch f]
  (bridge-in-order ch nil "receive-in-order"
    :wait-on-callback? true
    :callback
    (fn [msg]
      (p/run-pipeline msg
        {:error-handler (fn [ex]
                          (log/error ex "error in receive-in-order")
                          (p/complete nil))}
        f))))

(defn emit-in-order
  "Returns a channel that emits messages one at a time."
  [ch]
  (let [ch* (channel)]
    (bridge-in-order ch ch* "emit-in-order"
      :callback #(enqueue ch* %))

    ch*))

(defn take-
  [description n ch ch*]
  (let [n (long n)
        cnt (AtomicLong. 0)]
    (if (zero? n)

      (close ch*)

      (bridge-in-order ch ch* description
        
        :callback
        (fn [msg]
          (try
            (enqueue ch* msg)
            (finally
              (when (>= (.incrementAndGet cnt) n)
                (close ch*)))))))))

(defn take*
  "A dual to take.

   (take* 2 (channel 1 2 3)) => [1 2]"
  [n ch]
  (let [ch* (channel)]
    (take- "take*" n ch ch*)
    ch*))

(defn drop*
  "A dual to drop.

  (drop* 2 (closed-channel 1 2 3 4) => [3 4]"
  [n ch]
  (let [ch* (channel)]
    (p/run-pipeline nil
      {:error-handler (fn [_])}
      (fn [_]
        (take- "drop*" n ch (grounded-channel)))
      (fn [_]
        (bridge-join ch ch* "drop*"
          #(enqueue ch* %))))
    ch*))

(defn take-while*
  "A dual to take-while.

   (take-while* pos? (channel 1 2 0 4)) => [1 2]"
  [f ch]
  (let [ch* (mimic ch)]
    (bridge-in-order ch ch* "take-while*"
      :callback #(enqueue ch* %)
      :predicate f)
    ch*))

(defn reductions*
  "A dual to reductions.

   (reductions* max (channel 1 3 2)) => [1 3 3]"
  ([f ch]
     (let [ch* (mimic ch)]
       (p/run-pipeline (read-channel* ch :on-drained ::drained)
         {:error-handler (fn [ex] (error ch* ex false))}         
         (fn [val]
           (if (= ::drained val)

             ;; no elements, just invoke function
             (do
               (enqueue ch* (f))
               (close ch*))

             ;; reduce over channel
             (do
               (enqueue ch* val)
               (let [val (atom val)]
                 (bridge-in-order ch ch* "reductions*"
                   :callback #(enqueue ch* (swap! val f %))))))))
       ch*))
  ([f val ch]
     (let [ch* (mimic ch)
           val (atom val)]

       (enqueue ch* @val)
       (bridge-in-order ch ch* "reductions*"
         :callback #(enqueue ch* (swap! val f %)))

       ch*)))

(defn reduce*
  "A dual to reduce.  Returns a result-channel that emits the final reduced value
   when the source channel has been drained.

   (reduce* max (channel 1 3 2)) => 3"
  ([f ch]
     (p/run-pipeline (read-channel* ch :on-drained ::drained)
       {:error-handler (fn [_])}
       (fn [val]
         (if (= ::drained val)

           ;; no elements, just invoke function
           (r/success-result (f))

           ;; reduce over channel
           (reduce* f val ch)))))

  ([f val ch]
     (let [val (atom val)
           result (r/result-channel)]
       (p/run-pipeline nil
         {:error-handler (fn [_])
          :result result}
         (fn [_]
           (bridge-in-order ch nil "reduce*"
             :callback #(do (swap! val f %) nil)))
         (fn [_]
           @val))

       result)))


(defn partition-
  [n step ch description final-messages]
  (let [ch* (mimic ch)
        acc (atom [])
        result (atom (r/result-channel))]
    (p/run-pipeline nil
      {:error-handler (fn [ex] (error ch* ex false))}

      (fn [_]
        (bridge-in-order ch ch* description
          :on-complete
          (fn []
            (doseq [msg (final-messages @acc)]
              (enqueue ch* msg)))

          :callback
          (fn [msg]

            (if-not (= n (count (swap! acc conj msg)))

              ;; accumulate, and wait for the next
              @result

              ;; flush and advance
              (let [msgs @acc]
                (if (= n step)
                  (reset! acc [])
                  (swap! acc #(-> (drop step %) vec)))
                (p/run-pipeline nil
                  {:result @result
                   :finally #(reset! result (r/result-channel))}
                  (fn [_] (enqueue ch* msgs)))))))))

    ch*))

(defn partition*
  "A dual to partition.

   (partition* 2 (channel 1 2 3)) => [[1 2]]"
  ([n ch]
     (partition* n n ch))
  ([n step ch]
     (partition- n step ch "partition*"
       (constantly nil))))

(defn partition-all*
  "A dual to partition-all.

   (partition-all* 2 (closed-channel 1 2 3)) => [[1 2] [3]]"
  ([n ch]
     (partition-all* n n ch))
  ([n step ch]
     (partition- n step ch "partition-all*"
       #(partition-all n step %))))

;;;

(defn channel->lazy-seq-
  [read-fn cleanup-fn]
  (lazy-seq 
    (let [msg @(read-fn)]
      (if (= ::end msg)
        (do
          (cleanup-fn)
          nil)
        (cons msg (channel->lazy-seq- read-fn cleanup-fn))))))

(defn channel->lazy-seq
  "Returns a sequence.  As elements of the sequence are realized, messages from the
   source channel are consumed.  If there are no messages are available to be
   consumed, execution will block until one is available.

   A 'timeout' can be defined, either as a number or a no-arg function that returns a
   number.  Each time the seq must wait for a message to consume, it will only wait
   that many milliseconds before giving up and ending the sequence."
  ([ch]
     (channel->lazy-seq ch nil))
  ([ch timeout]
     (let [timeout-fn (when timeout
                        (if (number? timeout)
                          (constantly timeout)
                          timeout))
           e (g/edge "channel->lazy-seq" (g/terminal-propagator nil))]
       (if-let [unconsume (g/consume (emitter-node ch) e)]
         (channel->lazy-seq-
           (if timeout-fn
             #(read-channel* ch :timeout (timeout-fn), :on-timeout ::end, :on-drained ::end)
             #(read-channel* ch :on-drained ::end))
           unconsume)
         (throw (IllegalStateException. "Can't consume, channel already in use."))))))

(defn channel->seq
  "An eager variant of channel->lazy-seq.  Blocks until the channel has been drained,
   or until 'timeout' milliseconds have elapsed."
  ([ch]
     (g/drain (emitter-node ch)))
  ([ch timeout]
     (let [start (System/currentTimeMillis)
           s (g/drain (emitter-node ch))]
       (doall
         (concat s
           (channel->lazy-seq ch
             #(max 0 (- timeout (- (System/currentTimeMillis) start)))))))))

;;;

(defn concat*
  "A dual to concat.

   (concat* (channel [1 2] [2 3])) => [1 2 3 4]"
  [ch]
  (let [ch* (mimic ch)]
    (bridge-join ch ch* "concat*"
      (fn [s]
        (when-not (empty? s)
          (let [val (enqueue ch* (first s))]
            (doseq [msg (rest s)]
              (enqueue ch* msg))
            val))))
    ch*))

(defn mapcat*
  "A dual to mapcat.

   (mapcat* reverse (channel [1 2] [3 4])) => [2 1 4 3]"
  [f ch]
  (->> ch (map* f) concat*))

(defn transitions
  "Emits messages only when they differ from the preceding message."
  [ch]
  (let [ch* (mimic ch)
        r (AtomicReference. ::unmatchable)]
    (bridge-join ch ch* "transitions"
      (fn [msg]
        (if (= msg (.getAndSet r msg))
          :lamina/filtered
          (enqueue ch* msg))))
    ch*))

;;;

(defn periodically
  "Returns a channel.  Every 'period' milliseconds, 'f' is invoked with no arguments
   and the value is emitted as a message."
  ([period f]
     (periodically period f t/default-task-queue))
  ([period f task-queue]
     (let [ch (channel* :description (str "periodically " (describe-fn f)))
           cnt (atom 0)]
       (t/invoke-repeatedly task-queue period
         (fn [cancel-callback]
           (try
             (enqueue ch (f))
             (finally
               (when (closed? ch)
                 (cancel-callback))))))
       ch)))

(defn sample-every
  "Takes a source channel, and returns a channel that emits the most recent message
   from the source channel every 'period' milliseconds."
  ([period ch]
     (sample-every period t/default-task-queue ch))
  ([period task-queue ch]
     (let [val (atom ::none)
           ch* (mimic ch)]
       (bridge-join ch ch* (str "sample-every " period)
         #(reset! val %))
       (siphon
         (->> (periodically period #(deref val) task-queue)
           (remove* #(= ::none %)))
         ch*)
       ch*)))

(defn partition-every
  "Takes a source channel, and returns a channel that repeatedly emits a collection
   of all messages from the source channel in the last 'period' milliseconds."
  ([period ch]
     (partition-every period t/default-task-queue ch))
  ([period task-queue ch]
     (let [q (ConcurrentLinkedQueue.)
           drain (fn []
                   (loop [msgs []]
                     (if (.isEmpty q)
                       msgs
                       (let [msg (.remove q)]
                         (recur (conj msgs (if (identical? ::nil msg) nil msg)))))))
           ch* (mimic ch)]
       (bridge-join ch ch* (str "partition-every " period)
         #(.add q (if (nil? %) ::nil %)))
       (siphon
         (periodically period drain task-queue)
         ch*)
       ch*)))

;;;

(defn create-bitset [n] ;; todo: typehint with 'long'
  (let [bitset (BitSet. (long n))]
    (dotimes [idx n]
      (.set ^BitSet bitset idx))
    bitset))

(defn zip-all
  "something goes here"
  [& channels]
  (let [cnt (count channels)
        ^objects ary (object-array cnt)
        ^BitSet bitset (create-bitset cnt)
        lock (l/lock)
        ch* (channel* :description "zip-all")]
    
    (doseq [[idx ch] (map vector (range cnt) channels)] ;; todo: typehint with 'long'
      (bridge-join ch ch* ""
        (fn [msg]
          (if-let [ary* (l/with-exclusive-lock lock
                          (aset ary idx msg)
                          (when (or (.isEmpty bitset)
                                  (do
                                    (.set bitset (int idx) false)
                                    (.isEmpty bitset)))
                            (let [ary* (object-array cnt)]
                              (System/arraycopy ary 0 ary* 0 cnt)
                              ary*)))]
            (enqueue ch* (seq ary*))
            :lamina/incomplete))))

    ch*))

(defn zip
  "something goes here"
  [& channels]
  (let [cnt (count channels)
        lock (l/lock)
        ^objects ary (object-array cnt)
        bitset (atom (create-bitset cnt))
        result (atom (r/result-channel))
        ch* (channel)]
    (doseq [[idx ch] (map vector (range cnt) channels)] ;; todo: typehint with 'long'
      (bridge-siphon ch ch* "zip"
        (fn [msg]
          (let [curr-result @result]
            (if-let [ary* (l/with-exclusive-lock lock
                            (aset ary idx msg)
                            (let [^BitSet curr-bitset @bitset]
                              (.set curr-bitset (int idx) false)
                              (when (.isEmpty curr-bitset)
                                (reset! bitset (create-bitset cnt))
                                (reset! result (r/result-channel))
                                (let [ary* (object-array cnt)]
                                  (System/arraycopy ary 0 ary* 0 cnt)
                                  ary*))))]

              (p/run-pipeline nil
                {:error-handler (fn [_])
                 :result curr-result}
                (fn [_] (enqueue ch* (seq ary*))))

              curr-result)))))
    ch*))

(defn combine-latest
  "something goes here"
  [f & channels]
  (->> (apply zip-all channels)
    (map* #(apply f %))))

(defn merge-channels
  "something goes here"
  [& channels]
  (let [ch* (channel)
        cnt (atom (count channels))]
    (doseq [ch channels]
      (on-closed ch
        #(when (zero? (swap! cnt dec))
           (close ch*)))
      (siphon ch ch*))
    ch*))

;;;

(defn defer-onto-queue [task-queue time-facet ch]
  (let [ch* (channel)]
    (receive-in-order ch
      (fn [msg]
        (let [r (r/result-channel)]
          (t/invoke-at task-queue (time-facet msg)
            (fn []
              (try
                (enqueue ch* msg)
                (finally
                  (r/success r :lamina/consumed)))))
          r)))
    ch*))
