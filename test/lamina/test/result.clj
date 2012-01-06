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
    [clojure test])
  (:require
    [criterium.core :as c]))

(defmacro defer [& body]
  `(future
     (Thread/sleep 10)
     (try
       ~@body
       (catch Exception e#
         (.printStackTrace e#)))))

(defn capture-success
  ([result]
     (capture-success result :lamina/subscribed))
  ([result expected-return-value]
     (let [p (promise)]
       (is (= expected-return-value
             (subscribe result
               (result-callback
                 #(do (deliver p %) expected-return-value)
                 (fn [_] (throw (Exception. "ERROR")))))))
       p)))

(defn capture-error
  ([result]
     (capture-error result :lamina/subscribed))
  ([result expected-return-value]
     (let [p (promise)]
       (is (= expected-return-value
             (subscribe result
               (result-callback
                 (fn [_] (throw (Exception. "SUCCESS")))
                 #(do (deliver p %) expected-return-value)))))
       p)))

(deftest test-success-result
  (let [r (success-result 1)] 
    (is (= 1 @r))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= :lamina/already-realized! (success r nil)))
    (is (= :lamina/already-realized! (error r nil)))
    (is (= 1 @(capture-success r :foo)))
    (is (= false (cancel-callback r nil)))))

(deftest test-error-result
  (let [ex (IllegalStateException. "boom")
        r (error-result ex)]
    (is (thrown? IllegalStateException @r))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= :lamina/already-realized! (success r nil)))
    (is (= :lamina/already-realized! (error r nil)))
    (is (= ex @(capture-error r :foo)))
    (is (= false (cancel-callback r nil)))))

(deftest test-result-channel
  (let [r (result-channel)]
    (is (= ::none (success-value r ::none)))
    (is (= ::none (error-value r ::none)))
    (is (= nil (result r))))

  ;; success result
  (let [r (result-channel)]
    (is (= :lamina/realized (success r 1)))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= 1 @r)))

  ;; claim and success!
  (let [r (result-channel)]
    (is (= true (claim r)))
    (is (= :lamina/already-claimed! (success r 1)))
    (is (= :lamina/realized (success! r 1)))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= 1 @r)))
  
  ;; error result
  (let [r (result-channel)
        ex (IllegalStateException. "boom")]
    (is (= :lamina/realized (error r ex)))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (thrown? IllegalStateException @r)))

  ;; claim and error!
  (let [r (result-channel)
        ex (IllegalStateException. "boom")]
    (is (= true (claim r)))
    (is (= :lamina/already-claimed! (error r ex)))
    (is (= :lamina/realized (error! r ex)))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (thrown? IllegalStateException @r)))

  ;; test deref with success result
  (let [r (result-channel)]
    (defer (success r 1))
    (is (= 1 @r)))

  ;; test deref with error result
  (let [r (result-channel)]
    (defer (error r (IllegalStateException. "boom")))
    (is (thrown? IllegalStateException @r)))

  ;; multiple callbacks w/ success
  (let [r (result-channel)
        callback-values (->> (range 5)
                          (map (fn [_] (future (capture-success r))))
                          (map deref)
                          doall)]
    (is (= :lamina/split (success r 1)))
    (is (= 1 @r))
    (is (= (repeat 5 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [r (result-channel)
        callback-values (->> (range 5)
                          (map (fn [_] (future (capture-error r))))
                          (map deref)
                          doall)
        ex (Exception.)]
    (is (= :lamina/split (error r ex)))
    (is (thrown? Exception @r))
    (is (= (repeat 5 ex) (map deref callback-values))))

  ;; callback return result propagation in ::one
  (let [callback (result-callback (constantly :foo) nil)
        r (result-channel)]
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= :foo (success r nil))))

  ;; callback return result with ::many
  (let [callback (result-callback (constantly :foo) nil)
        r (result-channel)]
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= :lamina/split (success r nil))))

  ;; cancel-callback ::one to ::zero
  (let [callback (result-callback (constantly :foo) nil)
        r (result-channel)]
    (is (= false (cancel-callback r callback)))
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= true (cancel-callback r callback)))
    (is (= :lamina/realized (success r :foo)))
    (is (= :foo @(capture-success r)))
    (is (= false (cancel-callback r callback))))

  ;; cancel-callback ::many to ::one
  (let [cnt (atom 0)
        r (result-channel)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= :lamina/subscribed (subscribe r a)))
    (is (= :lamina/subscribed (subscribe r b)))
    (is (= true (cancel-callback r a)))
    (is (= 1 (success r nil)))
    (is (= 1 @cnt)))

  ;; cancel-callback ::many to ::many
  (let [cnt (atom 0)
        r (result-channel)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)
        c (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= :lamina/subscribed (subscribe r a)))
    (is (= :lamina/subscribed (subscribe r b)))
    (is (= :lamina/subscribed (subscribe r c)))
    (is (= true (cancel-callback r a)))
    (is (= :lamina/split (success r nil)))
    (is (= 2 @cnt))))

;;;

(defmacro bench [name & body]
  `(do
     (println "\n-----\n lamina.core.result -" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-result-channel
  (bench "create result-channel"
    (result-channel))
  (bench "subscribe and success"
    (let [r (result-channel)]
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench "subscribe, claim, and success!"
    (let [r (result-channel)]
      (subscribe r (result-callback (fn [_]) nil))
      (claim r)
      (success! r 1)))
  (bench "subscribe, cancel, subscribe and success"
    (let [r (result-channel)]
      (let [callback (result-callback (fn [_]) nil)]
        (subscribe r callback)
        (cancel-callback r callback))
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench "multi-subscribe and success"
    (let [r (result-channel)]
      (subscribe r (result-callback (fn [_]) nil))
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench "multi-subscribe, cancel, and success"
    (let [r (result-channel)]
      (subscribe r (result-callback (fn [_]) nil))
      (let [callback (result-callback (fn [_]) nil)]
        (subscribe r callback)
        (cancel-callback r callback))
      (success r 1)))
  (bench "success and subscribe"
    (let [r (result-channel)]
      (success r 1)
      (subscribe r (result-callback (fn [_]) nil))))
  (bench "claim, success!, and subscribe"
    (let [r (result-channel)]
      (claim r)
      (success! r 1)
      (subscribe r (result-callback (fn [_]) nil))))
  (bench "success and deref"
    (let [r (result-channel)]
      (success r 1)
      @r)))
