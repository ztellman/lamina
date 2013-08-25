;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.result
  (:use
    [potemkin]
    [lamina.core.utils])
  (:require
    [lamina.core.return-codes :as codes]
    [lamina.core.lock :as l]
    [lamina.time :as t])
  (:import
    [lamina.core.utils
     IEnqueue
     IError]
    [lamina.core.lock
     Lock]
    [java.util.concurrent
     ConcurrentLinkedQueue
     CountDownLatch
     CopyOnWriteArrayList]
    [java.util.concurrent.atomic
     AtomicInteger]
    [java.util
     ArrayList
     LinkedList]
    [java.io
     Writer]))

(deftype+ ResultCallback [on-success on-error])

(definterface+ IResult
  (success [_ val])
  (success! [_ val])
  (error! [_ err])
  (claim [_])
  (set-state [_ val])
  (success-value [_ default-value])
  (error-value [_ default-value])
  (add-listener [_ listener])
  (listeners [_])
  (result [_])
  (subscribe [_ callback])
  (cancel-callback [_ callback]))

(defn enqueue-to-listeners [^CopyOnWriteArrayList listeners result]
  (when-not (.isEmpty listeners)
    (if (= 1 (.size listeners))
      (enqueue (.get listeners 0) result)
      (doseq [listener listeners]
        (enqueue listener result)))))

(defn error-to-listeners [^CopyOnWriteArrayList listeners result]
  (when-not (.isEmpty listeners)
    (if (= 1 (.size listeners))
      (error (.get listeners 0) result false)
      (doseq [listener listeners]
        (error listener result false)))))

;;;

(declare result-channel)

(defmacro defer-within-transaction [defer-expr & body]
  `(if (lamina.core.utils/in-transaction?)
     (let [result# (result-channel)]
       (do
         (send (agent nil)
           (fn [_#]
             (try
               (lamina.core.result/success result# ~defer-expr)
               (catch Exception e#
                 (lamina.core.utils/log-error e# "Error in deferred action.")
                 (lamina.core.utils/error result# e# false)))))
         result#))
     (do ~@body)))

;;;

(deftype+ SuccessResult
  [value
   ^{:volatile-mutable true} metadata
   ^CopyOnWriteArrayList listeners]
  IEnqueue
  (enqueue [_ _]
    :lamina/already-realized!)

  IError
  (error [_ _ _]
    :lamina/already-realized!)

  clojure.lang.IDeref
  (deref [_] value)

  clojure.lang.IMeta
  clojure.lang.IReference
  (meta [_] metadata)
  (alterMeta [_ _ _] (throw (Exception. "not implemented, use .resetMeta instead")))
  (resetMeta [_ m] (set! metadata m))

  IResult
  (add-listener [_ listener]
    (.add listeners listener))
  (listeners [_]
    listeners)
  (success [_ _]
    :lamina/already-realized!)
  (success! [_ _]
    :lamina/already-realized!)
  (error! [_ _]
    :lamina/already-realized!)
  (claim [_]
    false)
  (success-value [_ default-value]
    (enqueue-to-listeners listeners :lamina/dereferenced)
    value)
  (error-value [_ default-value]
    default-value)
  (result [_]
    :success)
  (subscribe [_ callback]
    (let [result ((.on-success ^ResultCallback callback) value)]
      (enqueue-to-listeners listeners result)
      result))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< " (pr-str value) " >>")))

(deftype+ ErrorResult
  [error
   ^{:volatile-mutable true} metadata
   ^CopyOnWriteArrayList listeners]

  IEnqueue
  (enqueue [_ _]
    :lamina/already-realized!)

  IError
  (error [_ _ _]
    :lamina/already-realized!)

  clojure.lang.IDeref
  (deref [_]
    (if (instance? Throwable error)
      (throw error)
      (throw
        (or
          (codes/error-code->exception error)
          (Exception. (pr-str error))))))

  clojure.lang.IMeta
  clojure.lang.IReference
  (meta [_] metadata)
  (alterMeta [_ _ _] (throw (Exception. "not implemented, use .resetMeta instead")))
  (resetMeta [_ m] (set! metadata m))
  
  IResult
  (add-listener [_ listener]
    (.add listeners listener))
  (listeners [_]
    listeners)
  (success [_ _]
    :lamina/already-realized!)
  (success! [_ _]
    :lamina/already-realized!)
  (error! [_ _]
    :lamina/already-realized!)
  (claim [_]
    false)
  (success-value [_ default-value]
    default-value)
  (error-value [_ default-value]
    (enqueue-to-listeners listeners :lamina/dereferenced)
    error)
  (result [_]
    :error)
  (subscribe [_ callback]
    (let [result ((.on-error ^ResultCallback callback) error)]
      (enqueue-to-listeners listeners result)
      result))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< ERROR: " error " >>")))

;;;

(deftype+ ResultState [^long subscribers mode value claim-ref])

(defmacro update-state [^ResultState state signal value]
  `(let [signal# ~signal
         ^ResultState state# ~state
         mode# (if-let [ref# (.claim-ref state#)]
                 (if @ref# ::claimed (.mode state#))
                 (.mode state#))
         value# ~value]
     (if (and (identical? ::claim signal#) (in-transaction?))

       ;; do a transactional claim
       (when (identical? ::none mode#)
         (if-let [ref# (.claim-ref state#)]
           (do
             (ref-set ref# true)
             state#)
           (let [ref# (ref false)
                 state# (assoc-record ^ResultState state# :claim-ref ref#)]
             (ref-set ref# true)
             state#)))

       ;; all other cases
       (case mode#
         ::none
         (case signal#
           ::add (assoc-record ^ResultState state# :subscribers (inc (.subscribers state#)))
           ::remove (assoc-record ^ResultState state# :subscribers (dec (.subscribers state#)))
           ::success (assoc-record ^ResultState state# :mode ::success, :value value#)
           ::error (assoc-record ^ResultState state# :mode ::error, :value value#)
           (::success! ::error!) :lamina/not-claimed!
           ::claim
           (if-let [ref# (.claim-ref state#)]
             (if (dosync
                   (when-not (ensure ref#)
                     (ref-set ref# true)))
               (assoc-record ^ResultState state# :mode ::claimed)
               :lamina/:already-claimed!)
             (assoc-record ^ResultState state# :mode ::claimed)))
         
         ::claimed
         (case signal#
           ::add (assoc-record ^ResultState state# :subscribers (inc (.subscribers state#)))
           ::remove (assoc-record ^ResultState state# :subscribers (dec (.subscribers state#)))
           ::success! (assoc-record ^ResultState state# :mode ::success, :value value#)
           ::error! (assoc-record ^ResultState state# :mode ::error, :value value#)
           (::success ::error ::claim) :lamina/already-claimed!)
         
         (::success ::error)
         :lamina/already-realized!))))

;;;

(defmacro compare-and-trigger [[this this-f lock state subscribers listeners] signal f value & args]
  `(defer-within-transaction (~this-f ~this ~value ~@args)
     (let [value# ~value
           listeners# ~listeners
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
               (let [result# ((~f ^ResultCallback (.poll ~subscribers)) (.value s#))]
                 (enqueue-to-listeners listeners# result#)
                 result#)
               (catch Exception e#
                 (log-error e# "Error in result callback.")
                 (error-to-listeners listeners# e#)
                 :lamina/error!))
             
             (let [value# (.value s#)
                   ^{:tag "objects"} ary# (object-array (.size ~subscribers))]
                 
               (loop [idx# 0]
                 (when-let [^ResultCallback c# (.poll ~subscribers)]
                     
                   (try
                     (let [result# ((~f c#) value#)]
                       (enqueue-to-listeners listeners# result#)
                       (aset ary# idx# result#))
                     (catch Exception e#
                       (error-to-listeners listeners# e#)
                       (aset ary# idx# :lamina/error!)
                       (log-error e# "Error in result callback.")))
                     
                   (recur (unchecked-inc idx#))))

               (result-seq ary#))))))))

(defmacro def-result-channel [params & body]
  (let [{:keys [major minor]} *clojure-version*]
    `(deftype+ ~'ResultChannel
       ~params
       ~@(when-not (and (= 1 major) (= 2 minor))
           `(
             clojure.lang.IPending
             (isRealized [this#] (boolean (result this#)))
             
             clojure.lang.IBlockingDeref
             (deref [this# timeout-ms# timeout-val#]
               (let [r# (result-channel)]
                 (t/invoke-in timeout-ms# #(success r# timeout-val#))
                 (subscribe this#
                   (result-callback
                     #(success r# %)
                     #(error r# % false)))
                 @r#))))
       ~@body)))

(declare
  result-channel
  result-callback)

(def-result-channel
  [^Lock lock
   ^{:volatile-mutable true :tag ResultState} state
   ^CopyOnWriteArrayList listeners
   ^{:volatile-mutable true} metadata
   ^LinkedList subscribers]

  IEnqueue

  (enqueue [this msg]
    (success this msg))

  IError
  
  (error [this err _]
    (compare-and-trigger
      [this error lock state subscribers listeners]
      ::error .on-error err nil))

  ;;

  clojure.lang.IMeta
  clojure.lang.IReference
  
  (meta [_] metadata)
  (alterMeta [_ _ _] (throw (Exception. "not implemented, use .resetMeta instead")))
  (resetMeta [_ m] (set! metadata m))

  ;;

  clojure.lang.IDeref

  (deref [this]
    (enqueue-to-listeners listeners :lamina/deferenced)
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

  (add-listener [_ listener]
    (.add listeners listener))

  (listeners [_]
    listeners)

  ;;
  (success [this val]
    (compare-and-trigger
      [this success lock state subscribers listeners]
      ::success .on-success val)) 

  ;;
  (success! [this val]
    (compare-and-trigger
      [this success! lock state subscribers listeners]
      ::success! .on-success val))

  ;;
  (error! [this err]
    (compare-and-trigger
      [this error! lock state subscribers listeners]
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
        (do
          (enqueue-to-listeners listeners :lamina/dereferenced)
          (.value state))
        default-value)))

  ;;
  (error-value [_ default-value]
    (let [state state]
      (if (identical? ::error (.mode state))
        (do
          (enqueue-to-listeners listeners :lamina/dereferenced)
          (.value state))
        default-value)))

  ;;
  (result [_]
    (case (.mode state)
      ::success :success
      ::error :error
      nil))

  ;;
  (subscribe [this callback]
    (defer-within-transaction (subscribe this callback)
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
          (let [result (x (.value state))]
            (enqueue-to-listeners listeners result)
            result)))))

  ;;
  (cancel-callback [this callback]
    (defer-within-transaction (cancel-callback this callback)
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
  (SuccessResult. value nil (CopyOnWriteArrayList.)))

(defn error-result
  "Returns a result already realized with an error."
  [error]
  (ErrorResult. error nil (CopyOnWriteArrayList.)))

(defn result-channel
  "Returns a result-channel, representing an unrealized value or error."
  ([]
     (ResultChannel.
       (l/lock)
       (ResultState. 0 ::none nil nil)
       (CopyOnWriteArrayList.)
       nil
       (LinkedList.))))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

(defn async-promise?
  "Returns true if `x` is a result."
  [x]
  (or
    (instance? ResultChannel x)
    (instance? SuccessResult x)
    (instance? ErrorResult x)))

(defn siphon-result
  "When the source result is realized, that value or error is forwarded to the destination result-channel."
  [src dst]
  (subscribe src (result-callback #(success dst %) #(error dst % false)))
  dst)

(defn with-timeout
  "Returns a new result that will mimic the original result, unless `interval` milliseconds elapse, in which
   case it will realize as a 'lamina/timeout!' error."
  [interval result]
  (let [result* (siphon-result result (result-channel))]
    (if (zero? interval)
      (error result* :lamina/timeout! false)
      (t/invoke-in interval #(error result* :lamina/timeout! false)))
    result*))

(defn expiring-result
  "Returns a result-channel that will be realized as a 'lamina/timeout!' error if a value is not enqueued within
   `interval` milliseconds."
  ([interval]
     (expiring-result interval (t/task-queue)))
  ([interval task-queue]
     (let [result (result-channel)]
       (when interval
         (t/invoke-in task-queue interval
           #(error result :lamina/timeout! false)))
       result)))

(defn timed-result
  "Returns a result-channel that will be realized as `value` (defaulting to nil) in `interval` milliseconds."
  ([interval]
     (timed-result interval nil))
  ([interval value]
     (timed-result interval value (t/task-queue)))
  ([interval value task-queue]
     (let [result (result-channel)]
       (when interval
         (t/invoke-in task-queue interval
           #(success result value)))
       result)))

(defn merge-results
  "Given n `results` returns a single async-promise which will be realized as a sequence of all the realized
   results."
  [& results]
  (let [cnt (count results)
        counter (AtomicInteger. (inc cnt))
        ^objects ary (object-array cnt)
        combined-result (result-channel)]
    (loop [idx 0, results results]

      (if (empty? results)

        ;; no further results, decrement the counter one last time
        ;; and mark the success if everything else has been realized
        (if (zero? (.decrementAndGet counter))
          (success-result (seq ary))
          combined-result)
        
        (let [r (first results)]
          (if-not (async-promise? r)

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
                      (error combined-result err false))))
                (recur (inc idx) (rest results))))))))))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))

