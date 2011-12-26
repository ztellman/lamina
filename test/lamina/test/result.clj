;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.result
  (:use
    [lamina.core result]
    [clojure test]
    [criterium core]))

(defn capture-success
  ([result]
     (capture-success result true))
  ([result expected-return-value]
     (let [p (promise)]
       (is (= true (subscribe result (result-callback
                                       #(do (deliver p %) expected-return-value)
                                       (fn [_] (throw (Exception. "ERROR")))))))
       @p)))

(defn capture-error
  ([result]
     (capture-error result true))
  ([result expected-return-value]
     (let [p (promise)]
       (is (= true (subscribe result (result-callback
                                       (fn [_] (throw (Exception. "SUCCESS")))
                                       #(do (deliver p %) expected-return-value)))))
       @p)))

(deftest test-success-result
  (let [r (success-result 1)]
    (is (= true (success? r)))
    (is (= false (error? r)))
    (is (= 1 (result r)))
    (is (= false (success r nil)))
    (is (= false (error r nil)))
    (is (= 1 (capture-success r)))
    (is (= false (cancel-callback r nil)))))

(deftest test-error-result
  (let [r (error-result 1)]
    (is (= false (success? r)))
    (is (= true (error? r)))
    (is (= 1 (result r)))
    (is (= false (success r nil)))
    (is (= false (error r nil)))
    (is (= 1 (capture-error r)))
    (is (= false (cancel-callback r nil)))))

(deftest test-result-channel
  (let [r (result-channel)]
    (is (= false (success? r)))
    (is (= false (error? r))))

  (let [r (result-channel)]
    (is (= true (success r 1)))
    (is (= true (success? r)))
    (is (= false (error? r)))
    (is (= 1 (capture-success r ::return))))

  (let [r (result-channel)]
    (is (= true (error r 1)))
    (is (= false (success? r)))
    (is (= true (error? r)))
    (is (= 1 (capture-error r ::return))))

  ;; multiple callbacks w/ success
  (let [r (result-channel)
        callbacks (->> (range 5) (map (fn [_] (future (capture-success r)))) doall)]
    (is (= true (success r 1)))
    (is (= true (success? r)))
    (is (= false (error? r)))
    (is (= (repeat 5 1) (map deref callbacks))))

  ;; multiple callbacks w/ error
  (let [r (result-channel)
        callbacks (->> (range 5) (map (fn [_] (future (capture-error r)))) doall)]
    (is (= true (error r 1)))
    (is (= false (success? r)))
    (is (= true (error? r)))
    (is (= (repeat 5 1) (map deref callbacks))))

  ;; callback return result propagation in ::one
  (let [callback (result-callback (constantly :foo) nil)
        r (result-channel)]
    (is (= true (subscribe r callback)))
    (is (= :foo (success r nil))))

  ;; cancel-callback ::one to ::zero
  (let [callback (result-callback (constantly :foo) nil)
        r (result-channel)]
    (is (= false (cancel-callback r callback)))
    (is (= true (subscribe r callback)))
    (is (= true (cancel-callback r callback)))
    (is (= true (success r :foo)))
    (is (= :foo (capture-success r))))

  ;; cancel-callback ::many to ::one
  (let [cnt (atom 0)
        r (result-channel)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= true (subscribe r a)))
    (is (= true (subscribe r b)))
    (is (= true (cancel-callback r a)))
    (is (= 1 (success r nil)))
    (is (= 1 @cnt)))

  ;; cancel-callback ::many to ::many
  (let [cnt (atom 0)
        r (result-channel)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)
        c (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= true (subscribe r a)))
    (is (= true (subscribe r b)))
    (is (= true (subscribe r c)))
    (is (= true (cancel-callback r a)))
    (is (= true (success r nil)))
    (is (= 2 @cnt))))

(defn benchmarks [])
