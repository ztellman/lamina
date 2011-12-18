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
    [java.util.concurrent LinkedBlockingQueue]
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
  `(let [x# (l/exclusive-lock* ~lock
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
        ((~callback-slot x#) ~result)

        :else
        (do
          (doseq [callback# x#]
            ((~callback-slot ^ResultCallback callback#) ~result))
          true))))

(deftype ResultChannel [^AsymmetricLock lock
                        ^:unsynchronized-mutable value
                        ^:unsynchronized-mutable state
                        ^:unsynchronized-mutable callback-s]
  Result
  (success [_ val]
    (update-result-channel lock state value callback-s ::success val .on-success))
  (error [_ err]
    (update-result-channel lock state value callback-s ::error err .on-error))
  (success? [this]
    (l/non-exclusive-lock* lock
      (= ::success state)))
  (error? [_]
    (l/non-exclusive-lock* lock
      (= ::error state)))
  (result [_]
    (l/non-exclusive-lock* lock
      value))
  (subscribe [_ callback]
    (if-let [f (l/non-exclusive-lock* lock
                 (case state
                   ::error (.on-error ^ResultCallback callback)
                   ::success (.on-success ^ResultCallback callback)
                   (::zero ::one ::many) nil))]
      (f value)
      (when-let [f (l/exclusive-lock* lock
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
        (f value)))
    true)
  (cancel-callback [_ callback]
    (l/exclusive-lock* lock
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
                        true))))))
  (toString [_]
    (l/non-exclusive-lock lock
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
  (ResultChannel. (l/asymmetric-lock) nil ::zero nil))

(defn result-callback [on-success on-error]
  (ResultCallback. on-success on-error))

;;;

(defmethod print-method SuccessResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ErrorResult [o ^Writer w]
  (.write w (str o)))

(defmethod print-method ResultChannel [o ^Writer w]
  (.write w (str o)))
