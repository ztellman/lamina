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
    [java.util.concurrent LinkedBlockingQueue CountDownLatch]
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

(defmacro update-result-channel
  [lock state value callback-s
   state-value
   result
   callback-slot]
  `(let [x# (l/with-exclusive-lock* ~lock
              (case ~state
                ::zero    (do
                            (set! ~state ~state-value)
                            (set! ~value ~result)
                            nil)
                ::one     (do
                            (set! ~state ~state-value)
                            (set! ~value ~result)
                            ~callback-s)
                ::many    (do
                            (set! ~state ~state-value)
                            (set! ~value ~result)
                            ~callback-s)
                ::error   false
                ::success false))]
     (set! ~callback-s nil)
     (cond
        (= nil x#)
        true

        (= false x#)
        false

        (instance? ResultCallback x#)
        ((~callback-slot ^ResultCallback x#) ~result)

        :else
        (do
          (doseq [callback# x#]
            ((~callback-slot ^ResultCallback callback#) ~result))
          true))))

(deftype ResultChannel [^AsymmetricLock lock
                        ^:volatile-mutable value
                        ^:volatile-mutable state
                        ^:volatile-mutable callback-s]
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
      (update-result-channel lock state value callback-s ::success val .on-success)))
  (error [_ err]
    (io! "Cannot modify result-channels inside a transaction."
      (update-result-channel lock state value callback-s ::error err .on-error)))
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
      (if-let [f (l/with-non-exclusive-lock lock
                   (case state
                     ::error (.on-error ^ResultCallback callback)
                     ::success (.on-success ^ResultCallback callback)
                     nil))]
        (f value)
        (when-let [f (l/with-exclusive-lock* lock
                       (case state
                         ::error   (.on-error ^ResultCallback callback)
                         ::success (.on-success ^ResultCallback callback)
                         ::zero    (do
                                     (set! callback-s callback)
                                     (set! state ::one)
                                     nil)
                         ::one     (do
                                     (set! callback-s (list callback-s callback))
                                     (set! state ::many)
                                     nil)
                         ::many    (do
                                     (set! callback-s (cons callback callback-s))
                                     nil)))]
          (f value))))
    true)
  (cancel-callback [_ callback]
    (io! "Cannot modify result-channels inside a transaction."
      (l/with-exclusive-lock* lock
        (case state
          ::error   false
          ::success false
          ::zero    false
          ::one     (if (= callback callback-s)
                      (do
                        (set! callback-s nil)
                        (set! state ::zero)
                        true)
                      false)
          ::many    (let [x (loop [acc () remaining callback-s]
                              (if (empty? remaining)
                                false
                                (if (= callback (first remaining))
                                  (concat acc (rest remaining))
                                  (recur (cons (first remaining) acc) (rest remaining)))))]
                      (cond
                        (= false x)
                        false
                        
                        (->> x rest empty?)
                        (do
                          (set! state ::one)
                          (set! callback-s (first x))
                          true)
                        
                        :else
                        (do
                          (set! callback-s x)
                          true)))))))
  (toString [_]
    (l/with-non-exclusive-lock lock
      (case state
        ::error   (str "<< ERROR: " value " >>")
        ::success (str "<< " value " >>")
        (::zero ::one ::many) "<< ... >>"))))

;;;

(defn success-result [value]
  (SuccessResult. value))

(defn error-result [error]
  (ErrorResult. error))

(defn result-channel []
  (ResultChannel. (l/asymmetric-lock false) nil ::zero nil))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))
