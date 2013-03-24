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
    [lamina.core utils channel threads]
    [clojure.set :only [rename-keys]])
  (:require
    [lamina.core.graph.propagator :as dist]
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
    [lamina.core.channel
     Channel]
    [lamina.core.graph.propagator
     DistributingPropagator]
    [java.util.concurrent
     ConcurrentLinkedQueue
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicReferenceArray
     AtomicReference
     AtomicBoolean
     AtomicLong]
    [java.math
     BigInteger])) 

;;;

(defn bridge-in-order
  "A variant of `bridge` which guarantees that messages will be processed one at a time.  Useful for
   any operation which is sensitive to ordering, or difficult to write concurrently.

   Returns `dst`, which may be nil if the operation doesn't result in a derived stream.

   By default, closing `src` will close `dst`, but not vise-versa.

   Required parameters:

     `:callback` - the callback which receives each message, after `:predicate` returns true.

   Optional parameters:

     `:predicate` - a predicate which takes the next message, and returns whether it should be consumed.  If false,
                    `dst` is closed.
                    
     `:on-complete` - a callback which is invoked with zero parameters once the bridge is closed, but before `dst` is closed.

     `:close-on-complete?` - if true, forces an upstream connection, where closing `dst` closes `src`.

     `:wait-on-callback?` - if true, waits until the result from `callback` is realized before proceeding to the next message."
  [src dst description &
   {:keys [predicate
           callback
           on-complete
           close-on-complete?
           wait-on-callback?]
     :or {close-on-complete? false
	      wait-on-callback? false}}]

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
                          (when dst (close dst))
                          (when close-on-complete? (close src))))]
          
          (p/run-pipeline nil
            {:error-handler (fn [ex] (if dst
                                       (error dst ex false)
                                       (error src ex false)))
             :finally cleanup}

            (fn [_]
              (let [result (r/result-channel)]
                (p/run-pipeline nil
                  {:error-handler (fn [_])}
                  (fn [_]
                    (read-channel* src
                      :on-drained ::stop
                      :predicate predicate
                      :listener-result result
                      :on-false ::stop))
                  (fn [msg]
                    [result msg]))))

            (fn [[result msg]]
              (if (identical? ::stop msg)
                (p/complete nil)
                (let [f (fn []
                          (p/run-pipeline msg
                            {:result result
                             :error-handler (fn [_])}
                            callback))]
                  (r/defer-within-transaction (f)
                    (f))
                  (when wait-on-callback?
                    result))))
            
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
  "Consumes messages from the source channel, passing them to `f` one at a time.  If
   `f` returns a result-channel, consumption of the next message is deferred until
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

(defn drop-while*
  "A dual to drop-while.

  (drop-while* pos? (closed-channel 1 2 0 4) => [0 4]"
  [f ch]
  (let [ch* (channel)]
    (p/run-pipeline nil
      {:error-handler (fn [_])}
      (fn [_]
        (bridge-in-order ch nil "drop-while*"
          :callback (fn [_])
          :predicate f))
      (fn [_]
        (bridge-join ch ch* "drop-while*"
          #(enqueue ch* %))))
    ch*))

(defn reductions*
  "A dual to reductions.

   (reductions* max (channel 1 3 2)) => [1 3 3]"
  ([f ch]
     (let [ch (join ch (mimic ch))
           ch* (mimic ch)]
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
                   :callback #(enqueue ch* (swap! val f %))
                   :close-on-complete? true))))))
       ch*))
  ([f val ch]
     (let [ch* (mimic ch)
           val (atom val)]

       (enqueue ch* @val)
       (bridge-in-order ch ch* "reductions*"
         :callback #(enqueue ch* (swap! val f %))
         :close-on-complete? true)

       ch*)))

(defn reduce*
  "A dual to reduce.  Returns a result-channel that emits the final reduced value
   when the source channel has been drained.

   (reduce* max (channel 1 3 2)) => 3"
  ([f ch]
     (let [ch (join ch (mimic ch))]
       (p/run-pipeline (read-channel* ch :on-drained ::drained)
         {:error-handler (fn [_])}
         (fn [val]
           (if (= ::drained val)
             
             ;; no elements, just invoke function
             (r/success-result (f))
             
             ;; reduce over channel
             (reduce* f val ch))))))

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
          :close-on-complete? true
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

   A `timeout` can be defined, either as a number or a no-arg function that returns a
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
   or until `timeout` milliseconds have elapsed."
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
  "Returns a channel.  Every `period` milliseconds, `f` is invoked with no arguments
   and the value is emitted as a message."
  ([period f]
     (periodically period f (t/task-queue)))
  ([period f task-queue]
     (let [ch (channel* :description "periodically")
           cnt (atom 0)
           latch (-> f meta :close-latch)
           priority (or (-> f meta :priority) 0)]
       (t/invoke-repeatedly task-queue period
         (with-meta
           (fn [cancel-callback]
             (try
               (enqueue ch (f))
               (finally
                 (when (or (and latch @latch) (closed? ch))
                   (close ch)
                   (cancel-callback)))))
           {:priority priority}))
       ch)))

(defn bridge-accumulate
  "A variant of `bridge` which "
  [src dst description
   {:keys [period task-queue accumulator emitter]
    :or {task-queue (t/task-queue)
         period (t/period)}}]

  (let [begin-latch (AtomicBoolean. false)
        close-latch (atom false)
        close-callback #(close dst)]
    (on-closed src close-callback)
    (bridge-siphon src dst description
      (fn [msg]
        (try
          (accumulator msg)
          :lamina/accumulated
          (finally
            (when (.compareAndSet begin-latch false true)
              (cancel-callback src close-callback)
              (join
                (periodically period
                  (with-meta
                    (fn []
                      (try
                        (emitter)
                        (finally
                          (when (closed? src)
                            (reset! close-latch true)))))
                    {:close-latch close-latch})
                  task-queue)
                dst))))))
    dst))

(defn sample-every
  "Takes a source channel, and returns a channel that emits the most recent message
   from the source channel every `period` milliseconds."
  [{:keys [period task-queue] :as options} ch]
  (let [val (atom nil)]
    (bridge-accumulate ch (mimic ch) "sample-every"
      (merge options
        {:accumulator #(reset! val %)
         :emitter #(deref val)}))))

(defn partition-every
  "Takes a source channel, and returns a channel that repeatedly emits a collection
   of all messages from the source channel in the last `period` milliseconds."
  [{:keys [period task-queue] :as options} ch]
  (let [q (ConcurrentLinkedQueue.)]
    (bridge-accumulate ch (mimic ch) "partition-every"
      (merge options
        {:accumulator #(.add q (if (nil? %) ::nil %))
         :emitter (fn []
                    (let [cnt (count q)
                          ary (object-array cnt)]
                      (dotimes [i cnt]
                        (let [msg (.remove q)]
                          (aset ^objects ary i
                            (if (identical? ::nil msg)
                              nil
                              msg))))
                      (seq ary)))}))))

;;;

(defn create-bitset [n] ;; todo: typehint with `long`
  (let [bitset (BitSet. (long n))]
    (dotimes [idx n]
      (.set ^BitSet bitset idx))
    bitset))

(defn zip-all
  "For each message from one of the streams in `channels`, emits a tuple containing the most recent message
   from all streams.  In order for any tuple to be emitted, at least one message must have been emitted by
   all channels."
  [channels]
  (let [cnt (count channels)
        ^objects ary (object-array cnt)
        ^BitSet bitset (create-bitset cnt)
        lock (l/lock)
        ch* (channel* :description "zip-all")]
    
    (doseq [[^long idx ch] (map vector (range cnt) channels)]
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
  "Emits a tuple containing the most recent message from all `channels` once a single message has been received
   from each channel.

   Each stream in `channels` is assumed to be emitting at the same rate.  If this is not true, then the behavior
   for messages from higher-rate channels can be controlled via `most-frequent?`.

   Consider two channels, `a` and `b`, where `a` emits ascending numbers twice as fast as `b`. In this case, the output
   of each would look like this:

   `a`      `b`
    1        
    2        1
    3        
    4        2

    If `most-frequent?` is true, we will emit at the tempo of the highest-frequency channel, giving us the sequence:

    `[2, 1]`, `[3, 1]`, `[4, 2]`

    If `most-frequent?` is false, we will emit at the tempo of the lowest-frequency channel, giving us the sequence:

    `[2, 1]`, `[4, 2]`

    By default, `most-frequent?` is false.    
   "
  ([channels]
     (zip false channels))
  ([most-frequent? channels]
     (let [cnt (count channels)
           lock (l/lock)
           ^objects ary (object-array cnt)
           bitset (atom (create-bitset cnt))
           result (atom (r/result-channel))
           ch* (channel* :description "zip")
           copy-and-reset (fn []
                            (reset! bitset (create-bitset cnt))
                            (reset! result (r/result-channel))
                            (let [ary* (object-array cnt)]
                              (System/arraycopy ary 0 ary* 0 cnt)
                              ary*))]
       (doseq [[^long idx ch] (map vector (range cnt) channels)]
         (bridge-join ch ch* ""
           (fn [msg]
             (let [curr-result @result]
               (if-let [ary* (l/with-exclusive-lock lock
                               (let [^BitSet curr-bitset @bitset]
                                 (if (and most-frequent? (not (.get curr-bitset (int idx))))

                                   ;; copy the current array, and set the value for the next one
                                   (let [result (copy-and-reset)]
                                     (.set curr-bitset (int idx) false)
                                     (aset ary idx msg)
                                     result)

                                   ;; check if we've zeroed out the array
                                   (do
                                     (aset ary idx msg)
                                     (when (do
                                             (.set curr-bitset (int idx) false)
                                             (.isEmpty curr-bitset))
                                       (copy-and-reset))))))]

                 (p/run-pipeline nil
                   {:error-handler (fn [_])
                    :result curr-result}
                   (fn [_] (enqueue ch* (seq ary*))))

                 curr-result)))))
       ch*)))

(defn combine-latest
  "Given n-many `channels` and a function which takes n arguments, reevaluates the function for each new
   value emitted by one of the channels.  Effectively a composition of `zip-all` and `map*`."
  [f & channels]
  (->> (zip-all channels)
    (map* #(apply f %))))

(defn merge-channels
  "Combines n-many streams into a single stream.  The returned channel will be closed only
   once all source channels have been closed."
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

(defn defer-onto-queue
  "Takes an input `channel`, a `time-facet` which takes each message and returns the associated time, 
   and a `task-queue`.

   The returned channel will emit each message from `channel` only once the designated time has arrived.
   This assumes the timestamp for each message is monotonically increasing."
  [task-queue time-facet channel]
  (let [ch* (lamina.core.channel/channel)]
    (receive-in-order channel
      (fn [msg]
        (let [r (r/result-channel)]
          (t/invoke-at task-queue (time-facet msg)
            (with-meta
              (fn []
                (try
                  (enqueue ch* msg)
                  (finally
                    (r/success r :lamina/consumed))))
              {:priority Integer/MAX_VALUE}))
          r)))
    ch*))

;;;

(defn distributor
  "Returns a channel.

   Messages enqueued into this channel are split into multiple streams, grouped by
   (facet msg). When a new facet-value is encountered, (initializer facet ch)
   is called, allowing messages with that facet-value to be handled appropriately.

   If a facet channel is closed, it will be removed from the distributor, and
   a new channel will be generated when another message of that type is enqueued.
   This allows the use of (close-on-idle ch ...), if facet-values will change over
   time.

   Given messages with the form {:type :foo, :value 1}, to print the mean values of all
   types:

   (distributor
     {:facet       :type
      :initializer (fn [facet-value ch]
                     (siphon
                       (->> ch (map* :value) moving-average)
                       (sink #(println \"average for\" facet-value \"is\" %))))})"
  [{:keys [facet initializer on-clearance]}]
  (let [receiver (g/node identity)
        watch-channel (atom (fn [_]))
        propagator (g/distributing-propagator facet
                     (fn [id]
                       (let [ch (channel* :description (pr-str id))]
                         (@watch-channel (initializer id ch))
                         (receiver-node ch))))]

    (when on-clearance
      (let [facet-count (AtomicLong. 0)]
        (reset! watch-channel
          (fn [ch]
            (.incrementAndGet facet-count)
            (on-closed ch
              (fn []
                (when (and (zero? (.decrementAndGet facet-count))
                        (g/closed? receiver))
                  (on-clearance))))))))

    (g/join receiver propagator)
    
    (Channel. receiver receiver {::propagator propagator})))

(defn aggregate
  ""
  [{:keys [facet flush? task-queue period]
    :or {task-queue (t/task-queue)
         period (t/period)}}
   ch]
  (let [ch* (mimic ch)
        lock (l/lock)
        aggregator (atom (ConcurrentHashMap.))
        post-process #(-> (into {} %) (rename-keys {::nil nil}))]

    (bridge-siphon ch ch* "aggregate"
      (fn [msg]
        (let [id (facet msg)
              id* (if (nil? id)
                    ::nil
                    id)]
          (when-let [msg (l/with-exclusive-lock lock
                           (let [^ConcurrentHashMap m @aggregator]
                             (when (or
                                     ;; we've lapped ourselves
                                     (and (.putIfAbsent m id* msg)
                                       (reset! aggregator
                                         (doto (ConcurrentHashMap.)
                                           (.put id* msg))))
                                     
                                     ;; the flush predicate returns true
                                     (and flush?
                                       (flush? m)
                                       (reset! aggregator
                                         (ConcurrentHashMap.))))
                               m)))]
            (enqueue ch* (post-process msg))))))

    (on-closed ch
      (fn []
        (let [msg (post-process @aggregator)]
          (when-not (empty? msg)
            (enqueue ch* msg)))
        (close ch*)))

    ch*))

(defn distribute-aggregate
  "A mechanism similar to a SQL `group by` or the split-apply-combine strategy for data analysis.

   For each message from `channel`, the value returned by `(facet msg)` will be examined, and the
   message will be routed to a channel that consumes all messages with that facet value.  If no
   such channel exists, it will be generated by `(generator facet-value facet-channel)`, which takes
   the facet-value and associated channel, and returns an output channel which will be merged with
   the output of all other facet channels.

   The output of each facet channel is assumed to be periodic.  The `:period` may be specified, but
   is not required.

   Returns a channel which will periodically emit a map of facet-value onto the output of the generated
   facet-channel.

   Example:

     (distribute-aggregate 
	   {:facet     :uri
		:generator (fn [uri ch]
			          (rate ch))}
	   ch)
	
   will return a channel which periodically emits a map of the form
	
	  {\"/abc\" 2
	   \"/def\" 3}"
  [{:keys [facet generator period task-queue]
    :or {task-queue (t/task-queue)
         period (t/period)}}
   channel]
  (let [ch* (mimic channel)
        on-clearance (atom nil)
        dist (distributor
               {:facet facet
                :on-clearance (fn [] (@on-clearance))
                :initializer (fn [facet-value ch]
                               (let [generated (->> ch
                                                 (generator facet-value)
                                                 (map* #(vector facet-value %)))]
                                 (siphon generated ch*)
                                 generated))})
        dist-propagator (-> dist meta ::propagator)
        aggr (aggregate
               {:facet first
                :task-queue task-queue
                :period period
                :flush? #(and
                           (= (count %) (count dist-propagator))
                           (= (keys %) (dist/facets dist-propagator)))}
               ch*)]

    (join channel dist)
    (on-error aggr #(error dist % false))
    (on-closed aggr #(close dist))
    (reset! on-clearance #(close ch*))

    (let [out (map*
                #(zipmap (keys %) (map second (vals %)))
                aggr)

          latch (atom true)]

      ;; emit nil if no messages have come in yet
      (p/run-pipeline (read-channel* channel :on-drained nil)
        (fn [_]
          (reset! latch false)))

      (t/invoke-repeatedly task-queue period
        (fn [cancel]
          (if @latch
            (enqueue out nil)
            (cancel))))

      out)))
