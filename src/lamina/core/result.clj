;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.result
  (:use
    [useful.datatypes :only (assoc-record)]
    [lamina.core.utils])
  (:require
    [lamina.core.return-codes :as codes]
    [lamina.core.lock :as l]
    [lamina.core.threads :as t]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.lock
     Lock]
    [java.util.concurrent
     ConcurrentLinkedQueue
     CountDownLatch]
    [java.util.concurrent.atomic
     AtomicInteger]
    [java.util
     ArrayList
     LinkedList]
    [java.io
     Writer]))

(set! *warn-on-reflection* true)

(deftype ResultCallback [on-success on-error])

(defprotocol IResult
  (success [_ val])
  (error [_ err])
  (success! [_ val])
  (error! [_ err])
  (claim [_])
  (set-state [_ val])
  (success-value [_ default-value])
  (error-value [_ default-value])
  (result [_])
  (subscribe [_ callback])
  (cancel-callback [_ callback]))

;;;

(deftype SuccessResult [value]
  IEnqueue
  (enqueue [_ _]
    :lamina/already-realized!)
  clojure.lang.IDeref
  (deref [_] value)
  IResult
  (success [_ _]
    :lamina/already-realized!)
  (success! [_ _]
    :lamina/already-realized!)
  (error [_ _]
    :lamina/already-realized!)
  (error! [_ _]
    :lamina/already-realized!)
  (claim [_]
    false)
  (success-value [_ _]
    value)
  (error-value [_ default-value]
    default-value)
  (result [_]
    :success)
  (subscribe [_ callback]
    ((.on-success ^ResultCallback callback) value))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< " (pr-str value) " >>")))

(deftype ErrorResult [error]
  IEnqueue
  (enqueue [_ _]
    :lamina/already-realized!)
  clojure.lang.IDeref
  (deref [_]
    (if (instance? Throwable error)
      (throw error)
      (throw
        (or
          (codes/error-code->exception error)
          (Exception. (pr-str error))))))
  IResult
  (success [_ _]
    :lamina/already-realized!)
  (success! [_ _]
    :lamina/already-realized!)
  (error [_ _]
    :lamina/already-realized!)
  (error! [_ _]
    :lamina/already-realized!)
  (claim [_]
    false)
  (success-value [_ default-value]
    default-value)
  (error-value [_ _]
    error)
  (result [_]
    :error)
  (subscribe [_ callback]
    ((.on-error ^ResultCallback callback) error))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< ERROR: " error " >>")))

;;;

(deftype ResultState [^long subscribers mode value claim-ref])

(defmacro update-state [^ResultState state signal value]
  `(let [signal# ~signal
         ^ResultState state# ~state
         mode# (if-let [ref# (.claim-ref state#)]
                 (if @ref# ::claimed (.mode state#))
                 (.mode state#))
         value# ~value]
     (if (and (identical? ::claim signal#) (in-transaction?))
       (when (identical? ::none mode#)
         (if-let [ref# (.claim-ref state#)]
           (do
             (ref-set ref# true)
             state#)
           (let [ref# (ref false)
                 state# (assoc-record state# :claim-ref ref#)]
             (ref-set ref# true)
             state#)))
       (case mode#
         ::none
         (case signal#
           ::add (assoc-record state# :subscribers (inc (.subscribers state#)))
           ::remove (assoc-record state# :subscribers (dec (.subscribers state#)))
           ::success (assoc-record state# :mode ::success, :value value#)
           ::error (assoc-record state# :mode ::error, :value value#)
           (::success! ::error!) :lamina/not-claimed!
           ::claim
           (if-let [ref# (.claim-ref state#)]
             (if (dosync
                   (when-not (ensure ref#)
                     (ref-set ref# true)))
               (assoc-record state# :mode ::claimed)
               :lamina/:already-claimed!)
             (assoc-record state# :mode ::claimed)))
         
         ::claimed
         (case signal#
           ::add (assoc-record state# :subscribers (inc (.subscribers state#)))
           ::remove (assoc-record state# :subscribers (dec (.subscribers state#)))
           ::success! (assoc-record state# :mode ::success, :value value#)
           ::error! (assoc-record state# :mode ::error, :value value#)
           (::success ::error ::claim) :lamina/already-claimed!)
         
         (::success ::error)
         :lamina/already-realized!))))

;;;

(defmacro compare-and-trigger [[this this-f lock state subscribers] signal f value]
  `(defer-within-transaction [(~this-f ~this ~value) :lamina/deferred]
     (let [value# ~value
           s# (l/with-exclusive-lock ~lock
                (let [^ResultState s# (update-state ~state ~signal ~value)]
                  (if (keyword? s#)
                    s#
                    (set-state ~this s#))))]
       
       (if (keyword? s#)
         s#
         (let [^ResultState s# s#]
           (case (int (.subscribers s#))
             
             0
             :lamina/realized
             
             1
             (try
               ((~f ^ResultCallback (.poll ~subscribers)) (.value s#))
               (catch Exception e#
                 (log/error e# "Error in result callback.")
                 :lamina/error!))

             (let [value# (.value s#)
                   result# (try
                             ((~f ^ResultCallback (.poll ~subscribers)) value#)
                             (catch Exception e#
                               (log/error e# "Error in result callback.")
                               :lamina/error!))]
               (loop []
                 (when-let [^ResultCallback c# (.poll ~subscribers)]
                   (try
                     ((~f c#) value#)
                     (catch Exception e#
                       (log/error e# "Error in result callback.")))
                   (recur)))
               result#)))))))

(deftype ResultChannel
  [^Lock lock
   ^{:volatile-mutable true :tag ResultState} state
   ^LinkedList subscribers]

  IEnqueue

  (enqueue [this msg]
    (success this msg))

  clojure.lang.IDeref

  ;;
  (deref [this]
    (let [state state
          value (.value state)
          result (case (.mode state)
                   ::success value
                   ::error (if (instance? Throwable value)
                             (throw value)
                             (throw
                               (or
                                 (codes/error-code->exception value)
                                 (Exception. (str value)))))
                   ::none)]
      (if-not (identical? ::none result)
        result
        (io! "Cannot dereference an unrealized result-channel in a transaction"
          (let [^CountDownLatch latch (CountDownLatch. 1)
                f (fn [_] (.countDown latch))]
            (subscribe this (ResultCallback. f f))
            (.await latch)
            (deref this))))))
  
  IResult

  ;;
  (success [this val]
    (compare-and-trigger
      [this success lock state subscribers]
      ::success .on-success val)) 

  ;;
  (success! [this val]
    (compare-and-trigger
      [this success! lock state subscribers]
      ::success! .on-success val))

  ;;
  (error [this err]
    (compare-and-trigger
      [this error lock state subscribers]
      ::error .on-error err))

  ;;
  (error! [this err]
    (compare-and-trigger
      [this error! lock state subscribers]
      ::error! .on-error err))

  ;;
  (claim [this]
    (l/with-exclusive-lock lock
      (let [s (update-state state ::claim nil)]
        (if (instance? ResultState s)
          (do (set-state this s) true)
          false))))

  ;;
  (set-state [_ val]
    (set! state val)
    val)

  ;;
  (success-value [_ default-value]
    (let [state state]
      (if (identical? ::success (.mode state))
        (.value state)
        default-value)))

  ;;
  (error-value [_ default-value]
    (let [state state]
      (if (identical? ::error (.mode state))
        (.value state)
        default-value)))

  ;;
  (result [_]
    (case (.mode state)
      ::success :success
      ::error :error
      nil))

  ;;
  (subscribe [this callback]
    (defer-within-transaction [(subscribe this callback) :lamina/deferred]
      (let [^ResultCallback callback callback
            x (l/with-exclusive-lock lock
                (let [s state]
                  (case (.mode s)
                    ::success (.on-success callback)
                    ::error (.on-error callback)
                    (do
                      (.add subscribers callback)
                      (set-state this (update-state state ::add nil))
                      nil))))]
        (if (identical? nil x)
          :lamina/subscribed
          (x (.value state))))))

  ;;
  (cancel-callback [this callback]
    (defer-within-transaction [(cancel-callback this callback) :lamina/deferred]
      (l/with-exclusive-lock lock
        (let [s state]
          (case (.mode s)
            ::error   false
            ::success false
            (if (= 0 (.subscribers s))
              false
              (if (.remove subscribers callback)
                (do
                  (set-state this (update-state s ::remove nil))
                  true)
                false)))))))

  ;;
  (toString [_]
    (let [state state]
      (case (.mode state)
        ::error   (str "<< ERROR: " (pr-str (.value state)) " >>")
        ::success (str "<< " (pr-str (.value state)) " >>")
        "<< \u2026 >>"))))

;;;

(defn success-result
  "Returns a result already realized with a value."
  [value]
  (SuccessResult. value))

(defn error-result
  "Returns a result already realized with an error."
  [error]
  (ErrorResult. error))

(defn result-channel
  "Returns a result-channel, representing an unrealized value or error."
  []
  (ResultChannel.
    (l/lock)
    (ResultState. 0 ::none nil nil)
    (LinkedList.)))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

(defn async-result?
  "Returns true if 'x' is a result."
  [x]
  (or
    (instance? ResultChannel x)
    (instance? SuccessResult x)
    (instance? ErrorResult x)))

(defn siphon-result
  "When the source result is realized, that value or error is forwarded to the destination result-channel."
  [src dst]
  (subscribe src (result-callback #(success dst %) #(error dst %)))
  dst)

(defn with-timeout
  "Returns a new result that will mimic the original result, unless 'interval' milliseconds elapse, in which
   case it will realize as a 'lamina/timeout!' error."
  [interval result]
  (let [result* (siphon-result result (result-channel))]
    (if (zero? interval)
      (error result* :lamina/timeout!)
      (t/delay-invoke interval #(error result* :lamina/timeout!)))
    result*))

(defn expiring-result
  "Returns a result-channel that will be realized as a 'lamina/timeout!' error if a value is not enqueued within
   'interval' milliseconds."
  [interval]
  (if (zero? interval)
    (error-result :lamina/timeout!)
    (let [result (result-channel)]
      (t/delay-invoke interval #(error result :lamina/timeout!))
      result)))

(defn timed-result
  "Returns a result-channel that will be realized as 'value' (defaulting to nil) in 'interval' milliseconds."
  ([interval]
     (timed-result interval nil))
  ([interval value]
     (let [result (result-channel)]
       (t/delay-invoke interval #(success result value))
       result)))

(defn merge-results
  "something goes here"
  [& results]
  (let [cnt (count results)
        counter (AtomicInteger. (inc cnt))
        ary (object-array cnt)
        combined-result (result-channel)]
    (loop [idx 0, results results]

      (if (empty? results)

        ;; no further results, decrement the counter one last time
        ;; and mark the success if everything else has been realized
        (if (zero? (.decrementAndGet counter))
          (success-result (seq ary))
          combined-result)
        
        (let [r (first results)]
          (if-not (async-result? r)

            ;; not a result - set, decrement, and recur
            (do
              (aset ary idx r)
              (.decrementAndGet counter)
              (recur (inc idx) (rest results)))

            
            (case (result r)

              ;; just return the error
              :error
              r

              ;; resolved - set, decrement, and recur
              :success
              (do
                (aset ary idx (success-value r nil))
                (.decrementAndGet counter)
                (recur (inc idx) (rest results)))

              ;; unrealized - subscribe and recur
              (do
                (subscribe r
                  (result-callback
                    (fn [val]
                      (aset ary idx val)
                      (when (zero? (.decrementAndGet counter))
                        (success combined-result (seq ary))))
                    (fn [err]
                      (error combined-result err))))
                (recur (inc idx) (rest results))))))))))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))

