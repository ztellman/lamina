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

(defn test-queue [q]
  ;; enqueue, then receive
  (enqueue q 0 false)
  (enqueue q 1)
  (is (= 1 @(receive q)))

  ;; multi-receive, then enqueue
  (let [a (receive q)
        b (receive q)]
    (enqueue q 2)
    (is (= 2 @a))
    (is (= 2 @b)))

  ;; enqueue, then receive with predicate
  (enqueue q 3)
  (is (= ::nope @(receive q even? ::nope)))
  (is (= 1 @(receive q even? ::nope (r/success-result 1))))
  (is (= 3 @(receive q odd? nil)))

  ;; multi-receive with predicate, then enqueue
  (let [a (receive q odd? ::nope)
        b (receive q even? nil (r/success-result 1))
        c (receive q even? nil)]
    (enqueue q 4)
    (is (= ::nope @a))
    (is (= 1 @b))
    (is (= 4 @c)))

  ;; enqueue, then receive with faulty predicate
  (let [a (receive q (fn [_] (throw (Exception. "boom"))) nil)
        b (receive q (constantly true) nil)]
    (enqueue q 5)
    (is (thrown? Exception @a))
    (is (= 5 @b)))

  ;; receive, cancel, receive, and enqueue
  (let [a (receive q)]
    (is (= true (cancel-receive q a)))
    (is (= false (cancel-receive q (r/result-channel))))
    (let [b (receive q)]
      (enqueue q 6)
      (is (= 6 @b))))

  ;; multi-receive, cancel, and enqueue
  (let [a (receive q)
        b (receive q)]
    (is (= true (cancel-receive q b)))
    (enqueue q 7)
    (is (= 7 @a)))

  ;; receive with already claimed result-channel, then enqueue
  (receive q nil nil (r/success-result 1))
  (enqueue q 8)
  (is (= 8 @(receive q)))

  ;; enqueue, then receive with already claimed result-channel
  (enqueue q 9)
  ())

(deftest test-basic-queue
  (test-queue (q/queue)))

;;;

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.queue -" ~name "\n-----\n")
     (c/quick-bench
       (dotimes [_# (int 1e6)]
         ~@body)
       :reduce-with #(and %1 %2))))

(defn benchmark-queue [name q]
  (bench (str name " - receive and enqueue")
    (receive q)
    (enqueue q 1))
  (bench (str name " - receive with explicit result-channel and enqueue")
    (receive q nil nil (r/result-channel))
    (enqueue q 1))
  (bench (str name " - receive, cancel, receive and enqueue")
    (let [r (receive q nil nil)]
      (cancel-receive q r))
    (receive q)
    (enqueue q 1))
  (bench (str name " - multi-receive and enqueue")
    (receive q)
    (receive q)
    (enqueue q 1))
  (bench (str name " - multi-receive, cancel, and enqueue")
    (receive q)
    (let [r (receive q)]
      (cancel-receive q r))
    (enqueue q 1))
  (bench (str name " - enqueue and receive")
    (enqueue q 1)
    (receive q))
  (bench (str name " - enqueue and receive with explicit result-channel")
    (enqueue q 1)
    (receive q nil nil (r/result-channel)))
  (bench (str name "- enqueue without persistence")
    (q/enqueue q 1 false nil)))

(deftest ^:benchmark benchmark-basic-queue
  (benchmark-queue "basic-queue" (q/queue)))
