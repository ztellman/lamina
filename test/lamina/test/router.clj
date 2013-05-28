;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.router
  (:use
    [clojure.test]
    [lamina core viz]
    [lamina.cache :only (get-or-create)])
  (:require
    [lamina.trace :as tr]
    [lamina.query :as q]
    [lamina.time :as t]))

;;;

(defn next-msg [ch]
  (-> ch read-channel (wait-for-result 5e3)))

(defn next-non-zero-msg [ch]
  (->> (repeatedly #(next-msg ch))
    (drop-while zero?)
    first))

(defn next-non-empty-msg [ch]
  (->> (repeatedly #(next-msg ch))
    (drop-while empty?)
    first))

(defn close-all [chs]
  (doseq [ch chs]
    (close ch)))

(defmacro is* [& args]
  `(do
     (is ~@args)
     (print ".")
     (flush)))

(defn prepend [s x]
  (if (string? x)
    (str "&"  s x)
    (vec (concat [s] x))))

(defn run-test [{:keys [subscribe-fn enqueue-fn post-enqueue-fn consume-fn enqueued-values]} & expected-and-queries]
  (let [expected-and-queries (remove nil? expected-and-queries)
        queries (->> expected-and-queries
                  (partition 2)
                  (map second))
        query-channels (->> queries
                         (map (comp doall (partial map subscribe-fn)))
                         doall)
        expected (->> expected-and-queries
                   (partition 2)
                   (map first))]
    (try
        
      (doseq [v enqueued-values]
        (enqueue-fn v))

      (when post-enqueue-fn
        (post-enqueue-fn))

      (doseq [[val queries channels] (map vector expected queries query-channels)]
        (doseq [[query ch] (map vector queries channels)]
          (is* (= val (consume-fn ch)) (pr-str query))))

      (finally
        (close-all (apply concat query-channels))))))

;;;

(defn run-basic-operator-test [subscribe-fn enqueue-fn post-enqueue-fn]

  (run-test

    {:subscribe-fn subscribe-fn
     :enqueue-fn enqueue-fn
     :post-enqueue-fn post-enqueue-fn
     :consume-fn next-msg
     :enqueued-values (map
                        (fn [x] {:x {:y x}})
                        (range 1 5))}
  
    10.0
    [".x.y.sum()"
     '[:x :y sum]
     ".select(a: x.y, b: x).a.sum()"
     '[(select {:a (get-in :x :y), :b :x}) :a sum]
     ".x.y.sum(period: 0.1s).moving-average(period: 75000us)"]

    #_20.0
    #_[".merge(.x.y, .x.y).sum()"
     '[(merge [:x :y] [:x :y]) sum]]

    9.0
    [".where(x.y > 1).x.y.sum()"]

    4.0
    [".x.where(y = 4).y.sum()"]

    7.0
    [".x.where(y != 3).y.sum()"]

    7.0
    [".x.where(y not= 3).y.sum()"]

    6.0
    [".x.y.where(_ < 4).sum()"]

    7.0
    [".x.y.where(_ in [1 2 4]).sum()"]

    4
    [".rate()"]

    2.5
    [".x.y.moving-average(period: 75ms)"]

    1
    [".x.y"]))

(defn run-group-by-test [subscribe-fn enqueue-fn post-enqueue-fn]

  (run-test

    {:subscribe-fn subscribe-fn
     :enqueue-fn enqueue-fn
     :post-enqueue-fn post-enqueue-fn
     :consume-fn next-non-empty-msg
     :enqueued-values (map
                        #(hash-map :foo %1 :bar %2 :baz {:quux %2})
                        [:a :a :b :b :c]
                        [:x :x :z :y :y])}
    
    ;; tests
    {:a [:x :x], :b [:z :y], :c [:y]}
    [".group-by(foo).bar"]

    {:a 2, :b 2, :c 1}
    [".group-by(foo).rate()"]                      

    {:x 2, :y 2, :z 1}
    [".group-by(facet: bar).rate()"
     ".select(foo, bar).group-by(bar).rate()"
     ".select(bar).group-by(bar).bar.rate()"]

    {:c {:y 1}, :b {:y 1, :z 1}, :a {:x 2}}
    [".group-by(foo).select(bar).group-by(bar).rate()"]

    {[:a :x] 2, [:b :z] 1, [:c :y] 1, [:b :y] 1}
    [".group-by([foo bar]).rate()"
     ".group-by([foo baz.quux]).rate()"]

    {:x 2}
    [".where(foo = 'a').group-by(bar).rate()"
     ".where(foo ~= 'a').group-by(bar).rate()"]))

(defn run-merge-streams-test [subscribe-fn enqueue-fn post-enqueue-fn]
  (run-test

    {:subscribe-fn subscribe-fn
     :enqueue-fn enqueue-fn
     :post-enqueue-fn post-enqueue-fn
     :consume-fn next-msg
     :enqueued-values (map
                        #(hash-map :foo :bar, :x %, :y (* 2 %))
                        (range 1 5))}

    20.0
    [".merge(., &abc).x.sum()"]

    ;; todo: this is staggered in the split-router case
    #_{:a {:bar 10}, :b {:bar 10}}
    #_".zip(a: .group-by(foo).x.sum(),
            b: &abc.group-by(foo).x.sum())"))

;;;

(deftest test-operators
  (let [ch (channel)
        sub #(q/query-stream % {:period 100} ch)
        enq #(enqueue ch %)]
    (run-basic-operator-test sub enq nil) 
    (close ch))
  (let [ch (channel)
        sub #(q/query-stream % {:period 100} ch)
        enq #(enqueue ch %)]
    (run-group-by-test sub enq nil)
    (close ch))
  (println))

(deftest test-non-realtime-operators
  (let [ch (channel)
        sub #(q/query-stream %
               {:period 100, :timestamp (constantly 0)}
               (join ch (channel)))
        enq #(enqueue ch %)]
    (run-basic-operator-test sub enq #(close ch))
    (close ch))
  (let [ch (channel)
        q (t/non-realtime-task-queue 0 false)
        sub #(q/query-stream %
               {:period 1e5, :task-queue q, :auto-advance? true, :timestamp (constantly 0)}
               (join ch (channel)))
        enq #(enqueue ch %)]
    (run-group-by-test sub enq #(close ch))
    (close ch))
  (println))

(deftest test-non-realtime-router
  (let [q (t/non-realtime-task-queue)
        router (tr/trace-router
                 {:generator (fn [{:keys [pattern]}]
                               (tr/select-probes pattern))
                  :task-queue q
                  :payload :value
                  :timestamp :timestamp})
        sub #(tr/subscribe router (prepend "abc" %) {:period 1e5})
        enq #(tr/trace :abc {:timestamp 0, :value %})]
    (run-basic-operator-test sub enq #(t/advance-until q 2e5))
    (run-group-by-test sub enq #(t/advance-until q 5e5))
    (run-merge-streams-test sub enq #(t/advance-until q 5e5))
    (println)))

(deftest test-local-router
  (let [sub #(tr/subscribe tr/local-trace-router (prepend "abc" %) {:period 100})
        enq #(tr/trace :abc %)]
    (run-basic-operator-test sub enq nil)
    (run-group-by-test sub enq nil)
    (run-merge-streams-test sub enq nil)
    (println)))

(deftest test-split-router
  (let [router (tr/aggregating-trace-router tr/local-trace-router)
        sub #(tr/subscribe router (prepend "abc" %) {:period 100})
        enq #(tr/trace :abc %)]
    (run-basic-operator-test sub enq nil)
    (run-group-by-test sub enq nil)
    (run-merge-streams-test sub enq nil)
    (println)))
