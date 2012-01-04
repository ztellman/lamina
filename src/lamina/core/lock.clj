;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.lock
  (:import
    [java.util.concurrent Semaphore]))

(set! *warn-on-reflection* true)

;;;

(defprotocol AsymmetricLockProtocol
  (acquire [_])
  (acquire-exclusive [_])
  (release [_])
  (release-exclusive [_])
  (try-acquire [_])
  (try-acquire-exclusive [_]))

;;;

(deftype AsymmetricLock [^Semaphore semaphore]
  AsymmetricLockProtocol
  (acquire [_] (.acquire semaphore))
  (release [_] (.release semaphore))
  (acquire-exclusive [_] (.acquire semaphore Integer/MAX_VALUE))
  (release-exclusive [_] (.release semaphore Integer/MAX_VALUE))
  (try-acquire [_] (.tryAcquire semaphore))
  (try-acquire-exclusive [_] (.tryAcquire semaphore Integer/MAX_VALUE)))

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

;;;

(defn- rotations [s]
  (let [len (count s)]
    (map
      #(let [n (mod % len)]
         (concat (drop n s) (take n s)))
      (iterate inc 0))))

(defn- try-acquire-all [exclusive? locks]
  (let [f (if exclusive? try-acquire-exclusive try-acquire)]
    (loop [idx 0, s locks]
      (when-not (empty? s)
        (if-not (f (first s))
          idx
          (recur (inc idx) (rest s)))))))

(defn acquire-all
  "Acquires all locks, without chance of deadlock."
  [exclusive? locks]
  (let [a (if exclusive? acquire-exclusive acquire)
        r (if exclusive? release-exclusive release)]
    (when-not (empty? locks)
      (loop [ss (rotations locks)]
        (let [s (first ss)]
          ;; it's okay if we're interrupted here, we're not holding onto anything
          (a (first s)) 
          (when-let [n (try-acquire-all exclusive? (rest s))]
            (r (first s))
            (doseq [l (take n (rest s))]
              (r l))
            (recur (drop (inc n) ss))))))))
