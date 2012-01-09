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
    [java.util ArrayList]
    [java.io Writer]))

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

(deftype ResultState
  [mode ;; ::claimed, ::success, ::error
   subscriber-mode ;; ::zero, ::one, ::many
   subscribers
   value])

(defmacro compare-and-trigger! [old-mode new-mode lock state f value]
  `(io! "Cannot modify result-channels inside a transaction."
     (let [value# ~value
           s# ~state
           result# (l/with-exclusive-lock* ~lock
                     (case (.mode s#)
                       ~old-mode (do (set! ~state (ResultState. ~new-mode nil nil value#)) nil)
                       ~@(if (identical? ::none old-mode)
                           `(::claimed :lamina/already-claimed!)
                           `(::none :lamina/not-claimed!))
                       :lamina/already-realized!))]

       (if-not (identical? nil result#)
         result#
         (let [subscribers# (.subscribers s#)]
           (case (.subscriber-mode s#)
           
            ::zero
            :lamina/realized
           
            ::one
            ((~f ^ResultCallback subscribers#) value#)

            ::many
            (let [^ConcurrentLinkedQueue q# subscribers#]
              (loop []
                (when-let [^ResultCallback c# (.poll q#)]
                  ((~f c#) value#)
                  (recur)))
              :lamina/branch)))))))

(defmacro set-state! [state state-val & key-vals]
  `(let [val# (assoc-record ~state-val ~@key-vals)]
     (set! ~state val#)
     val#))

(deftype ResultChannel
  [^Lock lock
   ^{:volatile-mutable true :tag ResultState} state]

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
    (compare-and-trigger! ::none ::success lock state .on-success val))

  ;;
  (success! [_ val]
    (compare-and-trigger! ::claimed ::success lock state .on-success val))

  ;;
  (error [_ err]
    (compare-and-trigger! ::none ::error lock state .on-error err))

  ;;
  (error! [_ err]
    (compare-and-trigger! ::claimed ::error lock state .on-error err))

  ;;
  (claim [_]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-exclusive-lock* lock
        (let [s state]
          (if (identical? ::none (.mode s))
            (do
              (set-state! state s
                :mode ::claimed)
              true)
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
                      (case (.subscriber-mode s)
                        
                        ::zero
                        (set-state! state s
                          :subscriber-mode ::one
                          :subscribers callback)
                        
                        ::one
                        (set-state! state s
                          :subscriber-mode ::many
                          :subscribers (ConcurrentLinkedQueue. [(.subscribers s) callback]))
                        
                        ::many
                        (.add ^ConcurrentLinkedQueue (.subscribers s) callback))
                      :lamina/subscribed))))]
        (if-not (identical? :lamina/subscribed x)
          (x (.value state))
          x))))

  ;;
  (cancel-callback [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-exclusive-lock* lock
        (let [s state]
          (case (.mode s)
            ::error   false
            ::success false
            (case (.subscriber-mode s)
              
              ::zero
             false
            
             ::one
             (if-not (identical? callback (.subscribers s))
               false
               (do
                 (set-state! state s
                   :subscriber-mode ::zero
                   :subscribers nil)
                 true))
            
             ::many
             (let [^ConcurrentLinkedQueue q (.subscribers s)]
               (if-not (.remove ^ConcurrentLinkedQueue q callback)
                 false
                 (do
                   (when (identical? 1 (.size q))
                     (set-state! state s
                       ::subscriber-mode ::one
                       :subscribers (.poll q)))
                   true)))))))))

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
    (ResultState. ::none ::zero nil nil)))

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
