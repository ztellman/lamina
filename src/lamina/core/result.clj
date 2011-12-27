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
    [lamina.core.lock AsymmetricLock]
    [java.util.concurrent
     CopyOnWriteArrayList
     CountDownLatch]
    [java.util ArrayList]
    [java.io Writer]))

(deftype ResultCallback [on-success on-error])

(defprotocol Result
  (success [_ val])
  (error [_ err])
  (success? [_])
  (error? [_])
  (result [_])
  (subscribe [_ callback])
  (cancel-callback [_ callback]))

;;;

(deftype SuccessResult [value]
  clojure.lang.IDeref
  (deref [_] value)
  Result
  (success [_ _]
    false)
  (error [_ _]
    false)
  (success? [_]
    true)
  (error? [_]
    false)
  (result [_]
    value)
  (subscribe [_ callback]
    ((.on-success ^ResultCallback callback) value)
    true)
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< " value " >>")))

(deftype ErrorResult [error]
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
  (success? [_]
    false)
  (error? [_]
    true)
  (result [_]
    error)
  (subscribe [_ callback]
    ((.on-error ^ResultCallback callback) error)
    true)
  (cancel-callback [_ callback]
    false)
  (toString [_]
    (str "<< ERROR: " error " >>")))

(deftype ResultChannel [^AsymmetricLock lock
                        ^CopyOnWriteArrayList callbacks
                        ^:volatile-mutable value
                        ^:volatile-mutable state]
  clojure.lang.IDeref
  (deref [this]
    (if-let [result (l/with-non-exclusive-lock lock
                      (case state
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
        (case state
          ::success value
          ::error (if (instance? Throwable value)
                    (throw value)
                    (throw (Exception. (str value))))))))
  Result
  (success [_ val]
    (io! "Cannot modify result-channels inside a transaction."
      (if-not (l/with-exclusive-lock* lock
                (when (= ::none state)
                  (set! state ::success)
                  (set! value val)
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
      (if-not (l/with-exclusive-lock* lock
                (when (= ::none state)
                  (set! state ::error)
                  (set! value err)
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
  (success? [this]
    (l/with-non-exclusive-lock lock
      (= ::success state)))
  (error? [_]
    (l/with-non-exclusive-lock lock
      (= ::error state)))
  (result [_]
    (l/with-non-exclusive-lock lock
      value))
  (subscribe [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (when-let [f (l/with-non-exclusive-lock lock
                     (case state
                       ::error (.on-error ^ResultCallback callback)
                       ::success (.on-success ^ResultCallback callback)
                       ::none (do
                                (.add callbacks callback)
                                nil)))]
        (f value))
      true))
  (cancel-callback [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-non-exclusive-lock lock
        (case state
          ::error   false
          ::success false
          ::none    (.remove callbacks callback)))))
  (toString [_]
    (l/with-non-exclusive-lock lock
      (case state
        ::error   (str "<< ERROR: " value " >>")
        ::success (str "<< " value " >>")
        ::none    "<< ... >>"))))

;;;

(defn success-result [value]
  (SuccessResult. value))

(defn error-result [error]
  (ErrorResult. error))

(defn result-channel []
  (ResultChannel. (l/asymmetric-lock false) (CopyOnWriteArrayList.) nil ::none))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))
