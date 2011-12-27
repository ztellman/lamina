;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.queue
  (:use
    [lamina.core queue]
    [clojure test])
  (:require
    [lamina.core.result :as r]
    [criterium.core :as c]))

(defn test-queue [q]
  ;; enqueue, then receive
  (enqueue q 1)
  (is (= 1 @(receive q nil nil)))

  ;; multi-receive, then enqueue
  (let [a (receive q nil nil)
        b (receive q nil nil)]
    (enqueue q 2)
    (is (= 2 @a))
    (is (= 2 @b)))

  ;; enqueue, then receive with predicate
  (enqueue q 3)
  (is (= ::nope @(receive q even? ::nope)))
  (is (= 3 @(receive q odd? nil)))

  ;; multi-receive with predicate, then enqueue
  (let [a (receive q odd? ::nope)
        b (receive q even? nil)]
    (enqueue q 4)
    (is (= ::nope @a))
    (is (= 4 @b)))

  ;; receive, cancel, receive, and enqueue
  (let [a (receive q nil nil)]
    (cancel-callback q a)
    (let [b (receive q nil nil)]
      (enqueue q 5)
      (is (= 5 @b))))

  ;; multi-receive, cancel, and enqueue
  (let [a (receive q nil nil)
        b (receive q nil nil)]
    (cancel-callback q b)
    (enqueue q 6)
    (is (= 6 @a))))

(deftest test-basic-queue
  (test-queue (queue)))

;;;

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.queue -" ~name "\n-----\n")
     (c/bench
       (do
         ~@body)
       :reduce-with #(and %1 %2))))

(defn benchmark-queue [name q]
  (bench (str name " - receive and enqueue")
    (receive q nil nil)
    (enqueue q 1))
  (bench (str name " - receive, cancel, receive and enqueue")
    (let [r (receive q nil nil)]
      (cancel-callback q r))
    (receive q nil nil)
    (enqueue q 1))
  (bench (str name " - multi-receive and enqueue")
    (receive q nil nil)
    (receive q nil nil)
    (enqueue q 1))
  (bench (str name " - multi-receive, cancel, and enqueue")
    (receive q nil nil)
    (let [r (receive q nil nil)]
      (cancel-callback q r))
    (enqueue q 1))
  (bench (str name " - enqueue and receive")
    (enqueue q 1)
    (receive q nil nil)))

(deftest ^:benchmark benchmark-basic-queue
  (benchmark-queue "basic-queue" (queue)))
