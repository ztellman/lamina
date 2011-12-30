;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.lock
  (:import [java.util.concurrent Semaphore]))

(set! *warn-on-reflection* true)

;;;

(defprotocol AsymmetricLockProtocol
  (acquire [_])
  (release [_]))

;;;

;; the functions aren't really reentrant, but the macros are.  The important
;; thing is that we can have non-exclusive -> exclusive promotion, unlike
;; ReentrantReadWriteLocks
(deftype AsymmetricReentrantLock [^ThreadLocal permits ^Semaphore semaphore]
  AsymmetricLockProtocol
  (acquire [_]
    (let [p (long (or (.get permits) 0))]
      (when (<= Integer/MAX_VALUE p)
        (throw (IllegalStateException. "Cannot use 'acquire' while in exclusive-lock, use macros instead.")))
      (.acquire semaphore)
      (.set permits (unchecked-inc (long p)))))
  (release [_]
    (.release semaphore)
    (.set permits (unchecked-dec (long (.get permits))))))

(defn asymmetric-reentrant-lock []
  (AsymmetricReentrantLock. (ThreadLocal.) (Semaphore. Integer/MAX_VALUE)))

;;;

(defmacro with-reentrant-lock [lock & body]
  `(let [^AsymmetricReentrantLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#)
         ^ThreadLocal permits# (.permits lock#)
         acquire?# (= 0 (long (or (.get ^ThreadLocal permits#) 0)))]
     (do
       (when acquire?#
         (.acquire semaphore#)
         (.set permits# 1))
       (try
         ~@body
         (finally
           (when acquire?#
             (.release semaphore#)
             ;; we need to re-check the number of permits because (acquire ...) may have been called
             (.set permits#
               (let [p# (.get permits#)]
                 (unchecked-dec (long p#))))))))))

(defmacro with-exclusive-reentrant-lock [lock & body]
  `(let [^AsymmetricReentrantLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#) 
         ^ThreadLocal permits# (.permits lock#)
         acquired# (or (.get permits#) 0)
         to-acquire# (unchecked-subtract Integer/MAX_VALUE (long acquired#))
         acquire?# (< 0 to-acquire#)]
     (do
       (when acquire?#
         ;; if we don't first release all our existing permits, we can deadlock
         ;; with another exclusive-reentrant-lock that already has its own permits
         (when (> acquired# 0)
           (.release semaphore# acquired#))
         (.acquire semaphore# Integer/MAX_VALUE)
         (.set permits# Integer/MAX_VALUE))
       (try
         ~@body
         (finally
           (when acquire?#
             (.release semaphore# to-acquire#)
             ;; we don't need to re-check because (acquire ...) would have thrown an exception
             (.set permits# acquired#)))))))


;;;

(deftype AsymmetricLock [^Semaphore semaphore]
  AsymmetricLockProtocol
  (acquire [_] (.acquire semaphore))
  (release [_] (.release semaphore)))

(defn asymmetric-lock []
  (AsymmetricLock. (Semaphore. Integer/MAX_VALUE)))

;;;

(defmacro with-lock [lock & body]
  `(let [^AsymmetricLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#)]
     (do
       (.acquire semaphore#)
       (try
         ~@body
         (finally
           (.release semaphore#))))))

(defmacro with-exclusive-lock [lock & body]
  `(let [^AsymmetricLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#)]
     (do
       (.acquire semaphore# Integer/MAX_VALUE)
       (try
         ~@body
         (finally
           (.release semaphore# Integer/MAX_VALUE))))))

;; These variants exists because apparently try/catch, loop/recur, et al
;; close over the body, so using set! inside the body causes the compiler
;; to get confused.
;; 
;; Per http://dev.clojure.org/jira/browse/CLJ-274, this isn't going to get fixed
;; anytime soon, so should only be used where no exception can be thrown.  However,
;; an exception can still be thrown by interrupting the thread, so the body should
;; also always be uninterruptible.

(defmacro with-exclusive-lock* [lock & body]
  `(let [^AsymmetricLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#)]
     (.acquire semaphore# Integer/MAX_VALUE)
     (let [result# (do ~@body)]
       (.release semaphore# Integer/MAX_VALUE)
       result#)))

(defmacro with-lock* [lock & body]
  `(let [^AsymmetricLock lock# ~lock
         ^Semaphore semaphore# (.semaphore lock#)]
     (.acquire semaphore#)
     (let [result# (do ~@body)]
       (.release semaphore#)
       result#)))
