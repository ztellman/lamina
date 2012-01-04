;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.node
  (:use
    [clojure test])
  (:require
    [lamina.core.node :as n]
    [criterium.core :as c]))

;;;

(defn enqueue [n & msgs]
  (if (= 1 (count msgs))
    (n/propagate n (first msgs) true)
    (doseq [m msgs]
      (n/propagate n m true))))

(defn link [src dst]
  (n/link src dst dst))

(defn pred [f]
  (n/predicate-operator f))

(defn construct-nodes
  ([tree]
     (construct-nodes link tree))
  ([connect-fn [operator & downstream-operators]]
     (if (empty? downstream-operators)
       (n/callback-node operator)
       (let [n (n/node operator)]
         (doseq [d (map #(construct-nodes connect-fn %) downstream-operators)]
           (connect-fn n d))
         n))))

(defn node-chain [n final-callback]
  (let [s (repeatedly n #(n/node identity))]
    (doseq [[a b] (partition-all 2 1 s)]
      (if b
        (n/siphon a b)
        (when final-callback
          (link a (n/callback-node final-callback)))))
    (first s)))

(defn node-tree [depth branches leaf-callback]
  (if (zero? depth)
    (n/callback-node leaf-callback)
    (let [root (n/node identity)]
      (doseq [c (repeatedly branches
                  #(node-tree (dec depth) branches leaf-callback))]
        (if (n/node? c)
          (n/siphon root c)
          (link root c)))
      root)))

;;;

(defn sink []
  (let [a (atom [])]
    [a
     #(do
        (swap! a conj %)
        true)]))

(deftest test-simple-propagation
  ;; simple linear
  (let [[v callback] (sink)]
    (enqueue
      (construct-nodes [inc [(pred even?) [callback]]])
      1 2 3)
    (is (= @v [2 4])))

  ;; simple branched
  (let [[a callback-a] (sink)
        [b callback-b] (sink)]
    (enqueue
      (construct-nodes
        [identity
         [inc [(pred even?) [callback-a]]]
         [dec [(pred even?) [callback-b]]]])
      1 2 3)
    (is (= @a [2 4]))
    (is (= @b [0 2])))

  ;; deep branched
  (let [[v callback] (sink)]
    (enqueue (node-tree 9 2 callback) 1)
    (is (= @v (repeat 512 1)))))

(deftest test-queueing
  ;; simple test
  (let [n (n/node identity)
        [v f] (sink)]
    (is (= :lamina/enqueued (enqueue n nil)))
    (is (= :lamina/enqueued (enqueue n 1)))
    (link n (n/callback-node f))
    (is (= [nil 1] @v)))
  
  ;; test with closed node
  (let [n (n/node identity)
        [v f] (sink)]
    (is (= :lamina/enqueued (enqueue n nil)))
    (is (= :lamina/enqueued (enqueue n 1)))
    (n/close n)
    (is (n/closed? n))
    (is (not (n/drained? n)))
    (link n (n/callback-node f))
    (is (n/drained? n))
    (is (= [nil 1] @v))))

(deftest test-long-chain-propagation
  (let [n (node-chain 1e4 identity)]
    (is (= ::msg (enqueue n ::msg)))
    (-> n n/node-seq butlast last n/close)
    (let [latch (promise)]
      (n/on-closed n #(deliver latch true))
      (is (= true @latch)))))

;;;

(defmacro bench [n name & body]
  `(do
     (println "\n-----\n lamina.core.node -" ~name "\n-----\n")
     (c/quick-bench
       (dotimes [_# (int ~n)]
         ~@body)
       :reduce-with #(and %1 %2))))

(deftest ^:benchmark benchmark-node
  (bench 1e3 "create chain"
    (node-chain 1e3 identity))
  (let [n (node-chain 1e3 identity)]
    (bench 1e3 "linear propagation"
      (enqueue n true)))
  (let [n (node-tree 9 2 identity)]
    (bench 1e3 "tree propagation"
      (enqueue n true))))
