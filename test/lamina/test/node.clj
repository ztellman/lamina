;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.node
  (:use
    [clojure test]
    [lamina.core node])
  (:require
    [lamina.core.queue :as q]
    [criterium.core :as c]))

;;;

(defn enqueue [n & msgs]
  (if (= 1 (count msgs))
    (propagate n (first msgs) true)
    (doseq [m msgs]
      (propagate n m true))))

(defn link* [src dst]
  (link src dst dst))

(defn pred [f]
  (predicate-operator f))

(defn construct-nodes
  ([tree]
     (construct-nodes link* tree))
  ([connect-fn [operator & downstream-operators]]
     (if (empty? downstream-operators)
       (callback-node operator)
       (let [n (node operator)]
         (doseq [d (map #(construct-nodes connect-fn %) downstream-operators)]
           (connect-fn n d))
         n))))

(defn node-chain [n final-callback]
  (let [s (repeatedly n #(node identity))]
    (doseq [[a b] (partition-all 2 1 s)]
      (if b
        (siphon a b)
        (when final-callback
          (link* a (callback-node final-callback)))))
    (first s)))

(defn node-tree [depth branches leaf-callback]
  (if (zero? depth)
    (callback-node leaf-callback)
    (let [root (node identity)]
      (doseq [c (repeatedly branches
                  #(node-tree (dec depth) branches leaf-callback))]
        (if (node? c)
          (siphon root c)
          (link* root c)))
      root)))

(defn wait-for-closed [n]
  (let [latch (promise)]
    (on-closed n #(deliver latch true))
    (is (= true @latch))))

(defn wait-for-drained [n]
  (let [latch (promise)]
    (on-drained n #(deliver latch true))
    (is (= true @latch))))

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
  (let [n (node identity)
        [v f] (sink)]
    (is (= :lamina/enqueued (enqueue n nil)))
    (is (= :lamina/enqueued (enqueue n 1)))
    (link* n (callback-node f))
    (is (= [nil 1] @v)))
  
  ;; test with closed node
  (let [n (node identity)
        [v f] (sink)]
    (is (= :lamina/enqueued (enqueue n nil)))
    (is (= :lamina/enqueued (enqueue n 1)))
    (close n)
    (is (closed? n))
    (is (not (drained? n)))
    (link* n (callback-node f))
    (is (drained? n))
    (is (= [nil 1] @v))))

(deftest test-long-chain-propagation
  (let [n (node-chain 1e4 identity)]
    (is (= ::msg (enqueue n ::msg)))
    (-> n node-seq butlast last close)
    (wait-for-closed n)))

(deftest test-closing-backpropagation
  (let [a (node identity)
        b (node identity)
        c (node identity)]
    (siphon a b)
    (siphon a c)
    (enqueue a :msg)

    (close b)
    (close c)
    (wait-for-drained a)
    
    (is (= true (closed? b)))
    (is (= false (drained? b)))
    (is (= :msg @(predicate-receive b nil nil nil)))
    (is (= true (drained? b)))

    (is (= true (closed? c)))
    (is (= false (drained? c)))
    (let [[v callback] (sink)]
      (receive c callback callback)
      (is (= [:msg] @v)))
    (is (= true (drained? c)))))

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
