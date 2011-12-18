;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.lock
  (:import [java.util.concurrent Semaphore]))

(deftype AsymmetricReentrantLock [^ThreadLocal thread-local ^Semaphore semaphore])

(deftype AsymmetricLock [^Semaphore semaphore])

(defn asymmetric-reentrant-lock []
  (AsymmetricReentrantLock. (ThreadLocal.) (Semaphore. Integer/MAX_VALUE)))

(defn asymmetric-lock []
  (AsymmetricLock. (Semaphore. Integer/MAX_VALUE)))

(defmacro non-exclusive-reentrant-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricReentrantLock lock#)
         thread-local# (.thread-local ^AsymmetricReentrantLock lock#)
         acquire?# (= 0 (or (.get ^ThreadLocal thread-local#) 0))]
     (try
       (when acquire?#
         (.acquire ^Semaphore semaphore#)
         (.set ^ThreadLocal thread-local# 1))
       ~@body
       (finally
         (when acquire?#
           (.release ^Semaphore semaphore#)
           (.set ^ThreadLocal thread-local# 0))))))

(defmacro exclusive-reentrant-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricReentrantLock lock#) 
         thread-local# (.thread-local ^AsymmetricReentrantLock lock#)
         acquired# ^int (or (.get ^ThreadLocal thread-local#) 0)
         to-acquire# (- Integer/MAX_VALUE acquired#)]
     (try
       (.acquire ^Semaphore semaphore# to-acquire#)
       (.set ^ThreadLocal thread-local# Integer/MAX_VALUE)
       ~@body
       (finally
         (.release ^Semaphore semaphore# to-acquire#)
         (.set ^ThreadLocal thread-local# acquired#)))))

(defmacro non-exclusive-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (try
       (.acquire ^Semaphore semaphore#)
       ~@body
       (finally
         (.release ^Semaphore semaphore#)))))

(defmacro exclusive-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (try
       (.acquire ^Semaphore semaphore# Integer/MAX_VALUE)
       ~@body
       (finally
         (.release ^Semaphore semaphore# Integer/MAX_VALUE)))))

(defmacro exclusive-lock* [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (.acquire ^Semaphore semaphore# Integer/MAX_VALUE)
     (let [result# (do ~@body)]
       (.release ^Semaphore semaphore# Integer/MAX_VALUE)
       result#)))

(defmacro non-exclusive-lock* [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (.acquire ^Semaphore semaphore#)
     (let [result# (do ~@body)]
       (.release ^Semaphore semaphore#)
       result#)))

