;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.queue
  (:use
    [clojure test])
  (:require
    [lamina.core.queue :as q]
    [lamina.core.result :as r]
    [criterium.core :as c]))

(defn enqueue
  ([q msg]
     (q/enqueue q msg true nil))
  ([q msg persist?]
     (q/enqueue q msg persist? nil))
  ([q msg persist? release-fn]
     (q/enqueue q msg persist? release-fn)))

(defn receive
  ([q]
     (q/receive q nil nil nil))
  ([q predicate false-value]
     (q/receive q predicate false-value nil))
  ([q predicate false-value result-channel]
     (q/receive q predicate false-value result-channel)))

(defn cancel-receive [q callback]
  (q/cancel-receive q callback))

(defn test-queue [q-fn]

  ;; test ground
  (let [q (q-fn)]
    (enqueue q nil)
    (enqueue q :a)
    (is (= [nil :a] (q/ground q))))
  
  ;; enqueue, then receive
  (let [q (q-fn)]
    (enqueue q 0 false)
    (enqueue q nil)
    (is (= nil @(receive q))))

  ;; multi-receive, then enqueue
  (let [q (q-fn)
        a (receive q)
        b (receive q)]
    (enqueue q :a)
    (enqueue q :b)
    (is (= :a @a))
    (is (= :b @b)))

  ;; enqueue, then receive with predicate
  (let [q (q-fn)]
    (enqueue q 3)
    (is (= ::nope @(receive q even? ::nope)))
    (is (= 1 @(receive q even? ::nope (r/success-result 1))))
    (is (= 3 @(receive q odd? nil))))

  ;; multi-receive with predicate, then enqueue
  (let [q (q-fn)
        a (receive q odd? ::nope)
        b (receive q even? nil (r/success-result 1))
        c (receive q even? nil)]
    (enqueue q 2)
    (is (= ::nope @a))
    (is (= 1 @b))
    (is (= 2 @c)))

  ;; enqueue, then receive with faulty predicate
  (let [q (q-fn)
        a (receive q (fn [_] (throw (Exception. "boom"))) nil)
        b (receive q (constantly true) nil)]
    (enqueue q :msg)
    (is (thrown? Exception @a))
    (is (= :msg @b)))

  ;; receive, cancel, receive, and enqueue
  (let [q (q-fn)
        a (receive q)]
    (is (= true (cancel-receive q a)))
    (is (= false (cancel-receive q (r/result-channel))))
    (let [b (receive q)]
      (enqueue q 6)
      (is (= 6 @b))))

  ;; multi-receive, cancel, and enqueue
  (let [q (q-fn)
        a (receive q)
        b (receive q)]
    (is (= true (cancel-receive q a)))
    (enqueue q :msg)
    (is (= :msg @b)))

  ;; receive with already claimed result-channel, then enqueue
  (let [q (q-fn)]
    (receive q nil nil (r/success-result 1))
    (enqueue q 8)
    (is (= 8 @(receive q))))

  ;; enqueue, then receive with already claimed result-channel
  (let [q (q-fn)]
    (enqueue q 9)
    (receive q nil nil (r/success-result 1))
    (is (= 9 @(receive q)))))

(deftest test-basic-queue
  (test-queue #(q/queue)))

(deftest test-transactional-queue
  (test-queue #(q/transactional-queue)))

;;;

(defmacro bench [type name & body]
  `(do
     (println "\n-----\n lamina.core.queue -" ~type "-" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(defn benchmark-queue [type q r-fn]
  (bench type "receive and enqueue"
    (q/receive q)
    (enqueue q 1))
  (bench type "receive with explicit result-channel and enqueue"
    (receive q nil nil (r-fn))
    (enqueue q 1))
  (bench type "receive, cancel, receive and enqueue"
    (let [r (receive q nil nil)]
      (cancel-receive q r))
    (q/receive q)
    (enqueue q 1))
  (bench type "multi-receive and multi-enqueue"
    (q/receive q)
    (q/receive q)
    (enqueue q 1)
    (enqueue q 2))
  (bench type "multi-receive, cancel, and enqueue"
    (q/receive q)
    (let [r (q/receive q)]
      (cancel-receive q r))
    (enqueue q 1))
  (bench type "enqueue and receive"
    (enqueue q 1)
    (receive q))
  (bench type "enqueue and receive with explicit result-channel"
    (enqueue q 1)
    (receive q nil nil (r-fn)))
  (bench type "enqueue without persistence"
    (q/enqueue q 1 false nil)))

(deftest ^:benchmark benchmark-basic-queue
  (bench "" "create basic queue"
    (q/queue nil))
  (benchmark-queue "basic-queue"
    (q/queue nil)
    r/result-channel))


(deftest ^:benchmark benchmark-transactional-queue
  (bench "" "create transactional queue"
    (q/queue nil))
  (benchmark-queue "transactional-queue"
    (q/transactional-queue nil)
    r/transactional-result-channel))
