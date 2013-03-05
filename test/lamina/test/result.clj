;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.result
  (:use
    [lamina.core result utils]
    [lamina.time]
    [clojure test]
    [lamina.test utils]))

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
    (is (= :lamina/already-realized! (error r nil false)))
    (is (= 1 @(capture-success r :foo)))
    (is (= false (cancel-callback r nil))))

  (let [listener (result-channel)
        r (success-result 1)]
    (add-listener r listener)
    (is (= 1 @(capture-success r :foo)))
    (is (= :foo @listener))))

(deftest test-error-result
  (let [ex (IllegalStateException. "boom")
        r (error-result ex)]
    (is (thrown? IllegalStateException @r))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= :lamina/already-realized! (success r nil)))
    (is (= :lamina/already-realized! (error r nil false)))
    (is (= ex @(capture-error r :foo)))
    (is (= false (cancel-callback r nil))))

  (let [ex (IllegalStateException. "boom")
        listener (result-channel)
        r (error-result ex)]
    (add-listener r listener)
    (is (= ex @(capture-error r :foo)))
    (is (= :foo @listener))))

(defn test-result-channel [r-fn]
  (let [r (r-fn)]
    (is (= ::none (success-value r ::none)))
    (is (= ::none (error-value r ::none)))
    (is (= nil (result r))))

  ;; success result
  (let [r (r-fn)]
    (is (= :lamina/realized (success r 1)))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= 1 @r)))

  ;; success result with listener
  (let [listener (r-fn)
        r (r-fn)]
    (add-listener r listener)
    (is (= :lamina/realized (success r 1)))
    (is (= ::none (success-value r ::none)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= ::return @listener))
    (is (= 1 @r)))

  ;; claim and success!
  (let [r (r-fn)]
    (is (= true (claim r)))
    (is (= :lamina/already-claimed! (success r 1)))
    (is (= :lamina/realized (success! r 1)))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= 1 @r)))

  ;; transactional claim and success!
  (let [r (r-fn)]
    (is (= true (dosync (claim r))))
    (is (= :lamina/already-claimed! (success r 1)))
    (is (= :lamina/realized (success! r 1)))
    (is (= 1 (success-value r nil)))
    (is (= ::none (error-value r ::none)))
    (is (= :success (result r)))
    (is (= 1 @(capture-success r ::return)))
    (is (= 1 @r)))
  
  ;; error result
  (let [r (r-fn)
        ex (IllegalStateException. "boom")]
    (is (= :lamina/realized (error r ex false)))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (thrown? IllegalStateException @r)))

  ;; error result with listener
  (let [listener (r-fn)
        r (r-fn)
        ex (IllegalStateException. "boom")]
    (add-listener r listener)
    (is (= :lamina/realized (error r ex false)))
    (is (= ::none (success-value r ::none)))
    (is (= ::none (error-value r ::none)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (= ::return @listener))
    (is (thrown? IllegalStateException @r)))
  
  ;; claim and error!
  (let [r (r-fn)
        ex (IllegalStateException. "boom")]
    (is (= true (claim r)))
    (is (= :lamina/already-claimed! (error r ex false)))
    (is (= :lamina/realized (error! r ex)))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (thrown? IllegalStateException @r)))

  ;; transactional claim and error!
  (let [r (r-fn)
        ex (IllegalStateException. "boom")]
    (is (= true (dosync (claim r))))
    (is (= :lamina/already-claimed! (error r ex false)))
    (is (= :lamina/realized (error! r ex)))
    (is (= ::none (success-value r ::none)))
    (is (= ex (error-value r nil)))
    (is (= :error (result r)))
    (is (= ex @(capture-error r ::return)))
    (is (thrown? IllegalStateException @r)))

  ;; test deref with success result
  (let [r (r-fn)]
    (defer (success r 1))
    (is (= 1 @r)))

  ;; test deref with error result
  (let [r (r-fn)]
    (defer (error r (IllegalStateException. "boom") false))
    (is (thrown? IllegalStateException @r)))

  ;; multiple callbacks w/ success
  (let [n 50
        r (r-fn)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-success r))))
                          (map deref)
                          doall)]
    (is (= (repeat n :lamina/subscribed) (success r 1)))
    (is (= 1 @r))
    (is (= (repeat n 1) (map deref callback-values))))

  ;; multiple callbacks w/ error
  (let [n 50
        r (r-fn)
        callback-values (->> (range n)
                          (map (fn [_] (future (capture-error r))))
                          (map deref)
                          doall)
        ex (Exception.)]
    (is (= (repeat n :lamina/subscribed) (error r ex false)))
    (is (thrown? Exception @r))
    (is (= (repeat n ex) (map deref callback-values))))

  ;; callback return result propagation in ::one
  (let [callback (result-callback (constantly :foo) nil)
        r (r-fn)]
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= :foo (success r nil))))

  ;; callback return result with ::many
  (let [callback (result-callback (constantly :foo) nil)
        r (r-fn)]
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= [:foo :foo] (success r nil))))

  ;; cancel-callback ::one to ::zero
  (let [callback (result-callback (constantly :foo) nil)
        r (r-fn)]
    (is (= false (cancel-callback r callback)))
    (is (= :lamina/subscribed (subscribe r callback)))
    (is (= true (cancel-callback r callback)))
    (is (= :lamina/realized (success r :foo)))
    (is (= :foo @(capture-success r)))
    (is (= false (cancel-callback r callback))))

  ;; cancel-callback ::many to ::one
  (let [cnt (atom 0)
        r (r-fn)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= :lamina/subscribed (subscribe r a)))
    (is (= :lamina/subscribed (subscribe r b)))
    (is (= true (cancel-callback r a)))
    (is (= 1 (success r nil)))
    (is (= 1 @cnt)))

  ;; cancel-callback ::many to ::many
  (let [cnt (atom 0)
        r (r-fn)
        a (result-callback (fn [_] (swap! cnt inc)) nil)
        b (result-callback (fn [_] (swap! cnt inc)) nil)
        c (result-callback (fn [_] (swap! cnt inc)) nil)]
    (is (= :lamina/subscribed (subscribe r a)))
    (is (= :lamina/subscribed (subscribe r b)))
    (is (= :lamina/subscribed (subscribe r c)))
    (is (= true (cancel-callback r a)))
    (is (= [1 2] (success r nil)))
    (is (= 2 @cnt))))

(deftest test-basic-result-channel
  (test-result-channel result-channel))

;;;

(deftest ^:stress stress-test-merge-results
  (let [wait-for #(deref %2 %1 ::timeout)
        n 10]
    (dotimes* [i 1e6]
      (is (= (repeat n nil)
            (->> (repeatedly n result-channel)
              (map (fn [r]
                     (future
                       (Thread/sleep 0 (rand-int 1e3))
                       (success r nil))
                     r))
              (apply merge-results)
              (wait-for 500))))))) 

(defn stress-test-result-channel [r-fn]
  (dotimes* [i 1e5]
    (let [r (r-fn)]
      (invoke-in 0.1 (fn [] (success r i)))
      (Thread/sleep 1)
      (is (= i @r)))
    (let [r (r-fn)]
      (invoke-in 0.1 (fn [] (error r i false)))
      (Thread/sleep 1)
      (is (thrown? Exception @r)))))

(deftest ^:stress stress-test-basic-result-channel
  (println "\n----\n test result-channel \n---\n")
  (stress-test-result-channel result-channel))

;;;

(defn benchmark-result-channel [type r-fn]
  (bench (str type "create result-channel")
    (r-fn))
  (bench (str type "subscribe and success")
    (let [r (r-fn)]
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench (str type "subscribe, claim, and success!")
    (let [r (r-fn)]
      (subscribe r (result-callback (fn [_]) nil))
      (claim r)
      (success! r 1)))
  (bench (str type "subscribe, transactional claim, and success!")
    (let [r (r-fn)]
      (subscribe r (result-callback (fn [_]) nil))
      (dosync (claim r))
      (success! r 1)))
  (bench (str type "subscribe, cancel, subscribe and success")
    (let [r (r-fn)]
      (let [callback (result-callback (fn [_]) nil)]
        (subscribe r callback)
        (cancel-callback r callback))
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench (str type "multi-subscribe and success")
    (let [r (r-fn)]
      (subscribe r (result-callback (fn [_]) nil))
      (subscribe r (result-callback (fn [_]) nil))
      (success r 1)))
  (bench (str type "multi-subscribe, cancel, and success")
    (let [r (r-fn)]
      (subscribe r (result-callback (fn [_]) nil))
      (let [callback (result-callback (fn [_]) nil)]
        (subscribe r callback)
        (cancel-callback r callback))
      (success r 1)))
  (bench (str type "success and subscribe")
    (let [r (r-fn)]
      (success r 1)
      (subscribe r (result-callback (fn [_]) nil))))
  (bench (str type "claim, success!, and subscribe")
    (let [r (r-fn)]
      (claim r)
      (success! r 1)
      (subscribe r (result-callback (fn [_]) nil))))
  (bench (str type "success and deref")
    (let [r (r-fn)]
      (success r 1)
      @r)))

(deftest ^:benchmark benchmark-basic-result-channel
  (benchmark-result-channel "basic result - " result-channel))

