;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.result
  (:require
    [lamina.core.lock :as l])
  (:import
    [lamina.core.lock
     AsymmetricReentrantLock
     AsymmetricLockProtocol]
    [java.util.concurrent
     CopyOnWriteArrayList
     CountDownLatch]
    [java.util ArrayList]
    [java.io Writer]))

(set! *warn-on-reflection* true)

(deftype ResultCallback [on-success on-error])

(defprotocol Result
  (success [_ val])
  (error [_ err])
  (success-value [_ default-value])
  (error-value [_ default-value])
  (result [_])
  (subscribe [_ callback])
  (cancel-callback [_ callback]))

;;;

(deftype SuccessResult [value]
  AsymmetricLockProtocol
  (acquire [_])
  (release [_])
  clojure.lang.IDeref
  (deref [_] value)
  Result
  (success [_ _]
    false)
  (error [_ _]
    false)
  (success-value [_ _]
    value)
  (error-value [_ default-value]
    default-value)
  (result [_]
    :success)
  (subscribe [_ callback]
    ((.on-success ^ResultCallback callback) value)
    true)
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< " value " >>")))

(deftype ErrorResult [error]
  AsymmetricLockProtocol
  (acquire [_])
  (release [_])
  clojure.lang.IDeref
  (deref [_]
    (if (instance? Throwable error)
      (throw error)
      (throw (Exception. (str error)))))
  Result
  (success [_ _]
    false)
  (error [_ _]
    false)
  (success-value [_ default-value]
    default-value)
  (error-value [_ _]
    error)
  (result [_]
    :error)
  (subscribe [_ callback]
    ((.on-error ^ResultCallback callback) error)
    true)
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< ERROR: " error " >>")))

(deftype ResultState [type value])

(deftype ResultChannel
  [^AsymmetricReentrantLock lock
   ^CopyOnWriteArrayList callbacks
   ^:volatile-mutable ^ResultState state]

  AsymmetricLockProtocol
  (acquire [_]
    (l/acquire lock))
  (release [_]
    (l/release lock))
  
  clojure.lang.IDeref
  (deref [this]
    (if-let [result (let [state state
                          value (.value state)]
                      (case (.type state)
                        ::success value
                        ::error (if (instance? Throwable value)
                                  (throw value)
                                  (throw (Exception. (str value))))
                        nil))]
      result
      (let [latch (CountDownLatch. 1)
            f (fn [_] (.countDown ^CountDownLatch latch))]
        (subscribe this (ResultCallback. f f))
        (.await ^CountDownLatch latch)
        (let [state state
              value (.value state)]
          (case (.type state)
            ::success value
            ::error (if (instance? Throwable value)
                      (throw value)
                      (throw (Exception. (str value))))
            nil)))))
  
  Result
  (success [_ val]
    (io! "Cannot modify result-channels inside a transaction."
      (if-not (l/with-exclusive-reentrant-lock* lock
                (when (= ::none (.type state))
                  (set! state (ResultState. ::success val))
                  true))
        false
        (let [result (case (.size callbacks)
                       0 true
                       1 ((.on-success ^ResultCallback (.get callbacks 0)) val)
                       (do
                         (doseq [^ResultCallback c callbacks]
                           ((.on-success c) val))
                         true))]
          (.clear callbacks)
          result))))
  (error [_ err]
    (io! "Cannot modify result-channels inside a transaction."
      (if-not (l/with-exclusive-reentrant-lock* lock
                (when (= ::none (.type state))
                  (set! state (ResultState. ::error err))
                  true))
        false
        (let [result (case (.size callbacks)
                       0 true
                       1 ((.on-error ^ResultCallback (.get callbacks 0)) err)
                       (do
                         (doseq [^ResultCallback c callbacks]
                           ((.on-error c) err))
                         true))]
          (.clear callbacks)
          result))))
  (success-value [_ default-value]
    (let [state state]
      (if (= ::success (.type state))
        (.value state)
        default-value)))
  (error-value [_ default-value]
    (let [state state]
      (if (= ::error (.type state))
        (.value state)
        default-value)))
  (result [_]
    (case (.type state)
      ::success :success
      ::error :error
      nil))
  (subscribe [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (when-let [f (l/with-reentrant-lock lock
                     (case (.type state)
                       ::error (.on-error ^ResultCallback callback)
                       ::success (.on-success ^ResultCallback callback)
                       ::none (do
                                (.add callbacks callback)
                                nil)))]
        (f (.value state)))
      true))
  (cancel-callback [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-reentrant-lock lock
        (case (.type state)
          ::error   false
          ::success false
          ::none    (.remove callbacks callback)))))
  (toString [_]
    (let [state state]
      (case (.type state)
        ::error   (str "<< ERROR: " (.value state) " >>")
        ::success (str "<< " (.value state) " >>")
        ::none    "<< ... >>"))))

;;;

(defn success-result [value]
  (SuccessResult. value))

(defn error-result [error]
  (ErrorResult. error))

(defn result-channel []
  (ResultChannel.
    (l/asymmetric-reentrant-lock)
    (CopyOnWriteArrayList.)
    (ResultState. ::none nil)))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

(defn result-channel? [x]
  (or
    (instance? ResultChannel x)
    (instance? SuccessResult x)
    (instance? ErrorResult x)))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))
