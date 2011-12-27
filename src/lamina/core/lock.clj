;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.lock
  (:import [java.util.concurrent Semaphore]))

;;;

(deftype AsymmetricLock [^Semaphore semaphore])

(defn asymmetric-lock [fair?]
  (AsymmetricLock. (Semaphore. Integer/MAX_VALUE fair?)))

;;;

(defmacro with-non-exclusive-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (do
       (.acquire ^Semaphore semaphore#)
       (try
         ~@body
         (finally
           (.release ^Semaphore semaphore#))))))

(defmacro with-exclusive-lock [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (do
       (.acquire ^Semaphore semaphore# Integer/MAX_VALUE)
       (try
         ~@body
         (finally
           (.release ^Semaphore semaphore# Integer/MAX_VALUE))))))

(defmacro acquire-non-exclusive-lock [lock]
  `(let [lock# ~lock]
     (.acquire ^Semaphore (.semaphore ^AsymmetricLock lock#))))

(defmacro release-non-exclusive-lock [lock]
  `(let [lock# ~lock]
     (.release ^Semaphore (.semaphore ^AsymmetricLock lock#))))

;; These variants exists because apparently try/catch, loop/recur, et al
;; close over the body, so using set! inside the body causes the compiler
;; to get confused.
;; 
;; Per http://dev.clojure.org/jira/browse/CLJ-274, this isn't going to get fixed
;; anytime soon, so should only be used where no exception can be thrown.  However,
;; an exception can still be thrown by interrupting the thread, so the body should
;; also always be uninterruptible.

(defmacro with-exclusive-lock* [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (.acquire ^Semaphore semaphore# Integer/MAX_VALUE)
     (let [result# (do ~@body)]
       (.release ^Semaphore semaphore# Integer/MAX_VALUE)
       result#)))

(defmacro with-non-exclusive-lock* [lock & body]
  `(let [lock# ~lock
         semaphore# (.semaphore ^AsymmetricLock lock#)]
     (.acquire ^Semaphore semaphore#)
     (let [result# (do ~@body)]
       (.release ^Semaphore semaphore#)
       result#)))
