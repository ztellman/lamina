;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.lock
  (:use
    [potemkin])
  (:import
    [java.util.concurrent Semaphore]
    [java.util.concurrent.locks ReentrantLock ReentrantReadWriteLock]))



;;;

(definterface+ ILock
  (acquire [_])
  (acquire-exclusive [_])
  (release [_])
  (release-exclusive [_])
  (try-acquire [_])
  (try-acquire-exclusive [_]))

;;;

(deftype+ AsymmetricLock [^ReentrantReadWriteLock lock]
  ILock
  (acquire [this]
    (-> lock .readLock .lock))
  (release [this]
    (-> lock .readLock .unlock))
  (acquire-exclusive [this]
    (-> lock .writeLock .lock))
  (release-exclusive [this]
    (-> lock .writeLock .unlock))
  (try-acquire [_]
    (-> lock .readLock .tryLock))
  (try-acquire-exclusive [_]
    (-> lock .writeLock .tryLock)))

(defn asymmetric-lock []
  (AsymmetricLock. (ReentrantReadWriteLock. false)))

(deftype+ Lock [^ReentrantLock lock]
  ILock
  (acquire-exclusive [_] (.lock lock))
  (release-exclusive [_] (.unlock lock))
  (try-acquire-exclusive [_] (.tryLock lock)))

(defn lock []
  (Lock. (ReentrantLock. false)))

;;;

(defmacro with-lock [lock & body]
  `(let [lock# ~lock]
     (do
       (acquire lock#)
       (try
         ~@body
         (finally
           (release lock#))))))

(defmacro with-exclusive-lock [lock & body]
  `(let [lock# ~lock]
     (do
       (acquire-exclusive lock#)
       (try
         ~@body
         (finally
           (release-exclusive lock#))))))

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

(defn release-all
  [exclusive? locks]
  (let [f (if exclusive? release-exclusive release)]
    (doseq [l locks]
      (f l))))
