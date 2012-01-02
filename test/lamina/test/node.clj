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
    [lamina.core.node :as n]))

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

(defn sink []
  (let [a (atom [])]
    [a
     #(do
        (swap! a conj %)
        true)]))

(deftest test-simple-propagation
  (let [[v callback] (sink)]
    (enqueue
      (construct-nodes [inc [(pred even?) [callback]]])
      1 2 3)
    (is (= @v [2 4])))
  (let [[a callback-a] (sink)
        [b callback-b] (sink)]
    (enqueue
      (construct-nodes
        [identity
         [inc [(pred even?) [callback-a]]]
         [dec [(pred even?) [callback-b]]]])
      1 2 3)
    (is (= @a [2 4]))
    (is (= @b [0 2]))))

(deftest test-queueing
  (let [n (n/node identity)
        [v f] (sink)]
    (is (= :lamina/enqueued (enqueue n nil)))
    (is (= :lamina/enqueued (enqueue n 1)))
    (link n (n/callback-node f))
    (is (= [nil 1] @v))))







