;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.result
  (:use
    [useful.datatypes :only (assoc-record)])
  (:require
    [lamina.core.lock :as l]
    [lamina.core.threads :as t])
  (:import
    [lamina.core.lock
     Lock]
    [java.util.concurrent
     ConcurrentLinkedQueue
     CountDownLatch]
    [java.util
     ArrayList
     LinkedList]
    [java.io
     Writer]))

(set! *warn-on-reflection* true)

(deftype ResultCallback [on-success on-error])

(defprotocol Result
  (success [_ val])
  (error [_ err])
  (success! [_ val])
  (error! [_ err])
  (claim [_])
  (success-value [_ default-value])
  (error-value [_ default-value])
  (result [_])
  (subscribe [_ callback])
  (cancel-callback [_ callback]))

;;;

(deftype SuccessResult [value callback-modifier]
  clojure.lang.IDeref
  (deref [_] value)
  Result
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
    ((callback-modifier (.on-success ^ResultCallback callback)) value))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< " value " >>")))

(deftype ErrorResult [error callback-modifier]
  clojure.lang.IDeref
  (deref [_]
    (if (instance? Throwable error)
      (throw error)
      (throw (Exception. (pr-str error)))))
  Result
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
    ((callback-modifier (.on-error ^ResultCallback callback)) error))
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< ERROR: " error " >>")))

;;;

(defrecord ResultState [^long subscribers mode value])

(defmacro update-state [signal ^ResultState state value]
  `(let [signal# ~signal
         ^ResultState state# ~state
         mode# (.mode state#)
         value# ~value]
     (case mode#
       ::none
       (case signal#
         ::add (assoc-record state# :subscribers (inc (.subscribers state#)))
         ::remove (assoc-record state# :subscribers (dec (.subscribers state#)))
         ::claim (assoc-record state# :mode ::claimed)
         ::success (assoc-record state# :mode ::success, :value value#)
         ::error (assoc-record state# :mode ::error, :value value#)
         (::success! ::error!) :lamina/not-claimed!)
       
       ::claimed
       (case signal#
         ::success! (assoc-record state# :mode ::success, :value value#)
         ::error! (assoc-record state# :mode ::error, :value value#)
         (::success ::error) :lamina/already-claimed!)
       
       (::success ::error)
       :lamina/already-realized!)))

(defmacro compare-and-trigger! [[lock state subscribers] signal f value]
  `(io! "Cannot modify result-channels inside a transaction."
     (let [value# ~value
           s# ~state
           s# (l/with-exclusive-lock* ~lock
                (let [s# (update-state ~signal s# ~value)]
                  (if (keyword? s#)
                    s#
                    (do (set! ~state s#) s#))))]
       
       (if (keyword? s#)
         s#
         (let [^ResultState s# s#]
           (case (.subscribers s#)
             
             0
             :lamina/realized
             
             1
             ((~f ^ResultCallback (.removeFirst ~subscribers)) (.value s#))

             (let [value# (.value s#)]
               (loop []
                 (when-let [^ResultCallback c# (.poll ~subscribers)]
                   ((~f c#) value#)
                   (recur)))
               :lamina/branch)))))))

(deftype ResultChannel
  [^Lock lock
   ^{:volatile-mutable true :tag ResultState} state
   ^LinkedList subscribers]

  clojure.lang.IDeref

  ;;
  (deref [this]
    (if-let [result (let [state state
                          value (.value state)]
                      (case (.mode state)
                        ::success value
                        ::error (if (instance? Throwable value)
                                  (throw value)
                                  (throw (Exception. (str value))))
                        nil))]
      result
      (let [^CountDownLatch latch (CountDownLatch. 1)
            f (fn [_] (.countDown latch))]
        (subscribe this (ResultCallback. f f))
        (.await latch)
        (let [state state
              value (.value state)]
          (case (.mode state)
            ::success value
            ::error (if (instance? Throwable value)
                      (throw value)
                      (throw (Exception. (str value))))
            nil)))))
  
  Result

  ;;
  (success [_ val]
    (compare-and-trigger! [lock state subscribers] ::success .on-success val))

  ;;
  (success! [_ val]
    (compare-and-trigger! [lock state subscribers] ::success! .on-success val))

  ;;
  (error [_ err]
    (compare-and-trigger! [lock state subscribers] ::error .on-error err))

  ;;
  (error! [_ err]
    (compare-and-trigger! [lock state subscribers] ::error! .on-error err))

  ;;
  (claim [_]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-exclusive-lock* lock
        (let [s (update-state ::claim state nil)]
          (if (instance? ResultState s)
            (do (set! state s) true)
            false)))))

  ;;;

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

  ;;;

  ;;
  (subscribe [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (let [^ResultCallback callback callback
            x (l/with-exclusive-lock* lock
                (let [s state]
                  (case (.mode s)
                    ::success (.on-success callback)
                    ::error (.on-error callback)
                    (do
                      (.add subscribers callback)
                      (set! state (update-state ::add state nil))
                      nil))))]
        (if (identical? nil x)
          :lamina/subscribed
          (x (.value state))))))

  ;;
  (cancel-callback [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-exclusive-lock* lock
        (let [s state]
          (case (.mode s)
            ::error   false
            ::success false
            (if (identical? 0 (.subscribers state))
              false
              (if (.remove subscribers callback)
                (do
                  (set! state (update-state ::remove s nil))
                  true)
                false)))))))

  ;;
  (toString [_]
    (let [state state]
      (case (.mode state)
        ::error   (str "<< ERROR: " (.value state) " >>")
        ::success (str "<< " (.value state) " >>")
        "<< ... >>"))))

;;;

(defn success-result
  ([value]
     (success-result value identity))
  ([value callback-modifier]
     (SuccessResult. value callback-modifier)))

(defn error-result
  ([error]
     (error-result error identity))
  ([error callback-modifier]
     (ErrorResult. error callback-modifier)))

(defn result-channel
  []
  (ResultChannel.
    (l/lock)
    (ResultState. 0 ::none nil)
    (LinkedList.)))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

(defn result-channel? [x]
  (or
    (instance? ResultChannel x)
    (instance? SuccessResult x)
    (instance? ErrorResult x)))

(defn siphon-result [src dst]
  (subscribe src (result-callback #(success dst %) #(error dst %)))
  dst)

(defn result-timeout [interval result]
  (t/delay-invoke interval #(error result :lamina/timeout))
  result)

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))
