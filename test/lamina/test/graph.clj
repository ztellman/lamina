;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.graph
  (:use
    [clojure test]
    [lamina.test utils]
    [lamina.core.utils :only (predicate-operator error)]
    [lamina.core graph]
    [lamina.walk])
  (:require
    [lamina.core.queue :as q]))

;;;

(defn enqueue
  ([n msg]
     (propagate n msg true))
  ([n msg & msgs]
     (doall (map #(propagate n % true) (list* msg msgs)))))

(defn link* [src dst]
  (link src dst (edge "link" dst) nil nil))

(defn close* [& nodes]
  (doseq [n nodes]
    (close n false)))

(defn error* [node err]
  (error node err false))

(defn pred [f]
  (predicate-operator f))

(defn construct-nodes
  ([tree]
     (construct-nodes link* tree))
  ([connect-fn [operator & downstream-operators]]
     (if (empty? downstream-operators)
       (callback-propagator operator)
       (let [n (node operator)]
         (doseq [d (map #(construct-nodes connect-fn %) downstream-operators)]
           (connect-fn n d))
         n))))

(defn node-chain [n operator final-callback]
  (let [s (repeatedly n #(node operator))]
    (doseq [[a b] (partition-all 2 1 s)]
      (if b
        (siphon a b)
        (when final-callback
          (link* a (callback-propagator final-callback)))))
    (first s)))

(defn node-tree [depth branches leaf-callback]
  (if (zero? depth)
    (callback-propagator leaf-callback)
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

(defn wait-for-error [n err]
  (let [latch (promise)]
    (on-error n #(deliver latch %))
    (is (= err @latch))))

(defn sink []
  (let [a (atom [])]
    [a
     #(swap! a conj %)]))

;;;

(deftest test-simple-propagation
  ;; simple linear
  (let [[v callback] (sink)
        n (construct-nodes [inc [(pred even?) [callback]]])]
    (is (= [2] (enqueue n 1)))
    (enqueue n 2 3)
    (is (= @v [2 4])))

  ;; simple branched
  (let [[a callback-a] (sink)
        [b callback-b] (sink)
        n (construct-nodes
            [identity
             [inc [(pred even?) [callback-a]]]
             [dec [(pred odd?) [callback-b]]]])]
    (is (= [[2] :lamina/filtered]
          (enqueue n 1)))
    (is (= [[:lamina/filtered [1]] [[2 4] :lamina/filtered]]
          (enqueue n 2 3)))
    (is (= @a [2 4]))
    (is (= @b [1])))

  ;; deep branched
  (let [[v callback] (sink)]
    (enqueue (node-tree 9 2 callback) 1)
    (is (= @v (repeat 512 1)))))

(deftest test-receive
  (let [n (node identity)]
    (enqueue n 1)
    (let [[v callback] (sink)]
      (receive n nil callback)
      (is (= [1] @v)))
    (let [[v callback] (sink)]
      (receive n ::id nil)
      (cancel n ::id)
      (receive n callback callback)
      (enqueue n 2)
      (is (= [2] @v)))))

(deftest test-queueing
  ;; simple test
  (let [n (node identity)
        [v f] (sink)
        a (enqueue n nil)
        b (enqueue n 1)]
    (link* n (callback-propagator f))
    (is (= [nil 1] @v))
    (is (= [nil] @a))
    (is (= [nil 1] @b)))
  
  ;; test with closed node
  (let [n (node identity)
        [v f] (sink)
        a (enqueue n nil)
        b (enqueue n 1)]
    (close* n)
    (is (closed? n))
    (is (not (drained? n)))
    (link* n (callback-propagator f))
    (is (drained? n))
    (is (= [nil 1] @v))
    (is (= [nil] @a))
    (is (= [nil 1] @b))))

(deftest test-long-chain-propagation
  (let [cnt 1e4
        n (node-chain (dec cnt) inc inc)]
    (is (= (int cnt) (enqueue n 0)))
    (-> n node-seq butlast last close*)
    (wait-for-drained n)))

(defn closed-then-drained? [n f]
  (is (= true (closed? n)))
  (is (= false (drained? n)))
  (f n)
  (is (= true (drained? n))))

(deftest test-closing-backpropagation
  (let [a (node identity)
        b (node identity)
        c (node identity)
        d (node identity)]
    (siphon a b)
    (siphon a c)
    (siphon a d)
    (enqueue a :msg)

    (close* b c d)
    (wait-for-drained a)

    (closed-then-drained? b
      #(is (= :msg @(read-node % nil nil nil))))

    (closed-then-drained? c
      #(is (= :msg @(read-node %))))

    (closed-then-drained? d
      #(let [[v callback] (sink)]
         (receive % callback callback)
         (is (= [:msg] @v))))))

(deftest test-split
  (let [a (node identity)]
    (enqueue a 1 2 3)
    (is (= 1 @(read-node a)))

    (let [b (split a)]
      (is (= 2 @(read-node a)))
      (is (= 3 @(read-node b)))

      (receive a ::id nil)
      (cancel a ::id)
      (enqueue a 4)
      (let [[v callback] (sink)]
        (receive a nil callback)
        (is (= [4] @v)))

      (close* b)
      (wait-for-drained b)
      (wait-for-drained a))))

(defn join-and-siphon [f]
  (f join)
  (f siphon))

(deftest test-siphon-and-join
  (join-and-siphon
    #(let [a (node identity)
           b (node identity)]
       (% a b)
       (enqueue a 1 2 3)
       (is (= [1 2 3] (drain b)))
       (close* b)
       (wait-for-drained b)
       (wait-for-drained a)))

  (join-and-siphon
    #(let [a (node identity)
           b (node identity)]
       (% a b)
       (error* b ::error)
       (wait-for-error b ::error)
       (if (= siphon %)
         (wait-for-drained a)
         (wait-for-error a ::error))))

  (join-and-siphon
    #(let [a (node identity)
           b (node identity)]
       (close* b)
       (% a b)
       (wait-for-drained a)))

  (join-and-siphon
    #(let [a (node identity)
           b (node identity)]
       (close* b)
       (% a b)
       (wait-for-drained a)))

  (let [a (node identity)
        b (node identity)]
    (join a b)
    (close* a)
    (wait-for-drained a)
    (wait-for-drained b))

    (join-and-siphon
    #(let [a (node identity)
           b (node identity)]
       (close* b)
       (% a b)
       (wait-for-drained a)))

  (let [a (node identity)
        b (node identity)]
    (close* a)
    (join a b)
    (wait-for-drained a)
    (wait-for-drained b))

  (let [a (node identity)
        b (node identity)]
    (join a b)
    (error* a ::error)
    (wait-for-error a ::error)
    (wait-for-error b ::error)))

;;;

(deftest ^:benchmark benchmark-node
  (bench "create node"
    (node identity))
  (let [n (node* :probe? true)]
    (bench "inactive probe"
      (enqueue n true)))
  (bench "create and siphon"
    (let [a (node identity)
          b (node identity)]
      (siphon a b)))
  (bench "create and join"
    (let [a (node identity)
          b (node identity)]
      (join a b)))
  (let [n (node-chain 9 identity identity)]
    (bench "short propagation"
      (enqueue n true)))
  (let [n (node-chain 1e3 identity identity)]
    (bench "linear propagation"
      (enqueue n true)))
  (let [n (node-tree 9 2 identity)]
    (bench "tree propagation"
      (enqueue n true))))
