;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.connections
  (:use
    [lamina core connections]
    [lamina.test utils]
    [clojure test]))

(defn simple-echo-server []
  (let [[a b] (channel-pair)]
    (receive-all b #(enqueue b %))
    (on-drained b #(close a))
    [a #(close b)]))

(defn delayed-echo-server [delay]
  (let [[a b] (channel-pair)]
    (receive-all b
      #(run-pipeline nil
         (wait-stage delay)
         (fn [_] (enqueue b %))))
    (on-drained b #(close a))
    [a #(close b)]))

(defn test-echo [client-fn]
  (let [c (client-fn (comp first simple-echo-server))]
    (= 1 @(c 1))))

(defn test-timeout [client-fn]
  (let [c (client-fn #(first (delayed-echo-server 100)))]
    (is (thrown? Exception @(c 1 50)))
    (is (= 1 @(c 1 500)))))

(defn test-heartbeat [client-fn]
  (let [latch (promise)
        c (client-fn #(first (delayed-echo-server 100))
            {:heartbeat {:request :a
                         :interval 0
                         :timeout 10
                         :on-failure (fn [_] (deliver latch true))}})]
    (is (= true @latch))
    (close-connection c))
  (let [latch (promise)
        c (client-fn (comp first simple-echo-server)
            {:heartbeat {:request :a
                         :interval 0
                         :response-validator #(= % :b)
                         :on-failure (fn [_] (deliver latch true))}})]
    (is (= true @latch))
    (close-connection c)))

(defn test-client-fn [client-fn]
  (let [test-fns [test-echo
                  test-timeout
                  test-heartbeat]]
    (doseq [f test-fns]
      (f client-fn))))

(deftest test-client
  (test-client-fn client))

(deftest test-pipelined-client
  (test-client-fn pipelined-client))

;;;

(defn echo-handler [ch r]
  (enqueue ch r))

(defn setup-client [client-fn server-fn handler]
  (let [[a b] (channel-pair)]
    (server-fn handler b {})
    (client-fn (constantly a))))

(defn run-server-tests [server-fn]
  (let [c (setup-client client server-fn echo-handler)]
    (is (= 1 @(c 1)))))

(deftest test-server
  (run-server-tests server))

(deftest test-pipelined-server
  (run-server-tests pipelined-server))

;;;

(deftest ^:benchmark benchmark-clients
  (let [c (client (comp first simple-echo-server))]
    (bench "normal client"
      @(c 1)))
  (let [c (pipelined-client (comp first simple-echo-server))]
    (bench "pipelined client"
      @(c 1)))
  (let [c (setup-client client server echo-handler)]
    (bench "normal client, normal server"
      @(c 1)))
  (let [c (setup-client client pipelined-server echo-handler)]
    (bench "normal client, pipelined server"
      @(c 1)))
  (let [c (setup-client pipelined-client server echo-handler)]
    (bench "pipelined client, normal server"
      @(c 1)))
  (let [c (setup-client pipelined-client pipelined-server echo-handler)]
    (bench "pipelined client, pipelined server"
      @(c 1))))
