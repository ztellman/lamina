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
    [lamina.trace.router]
    [lamina core viz]
    [lamina.cache :only (get-or-create subscribe)]
    [lamina.trace :only (trace select-probes probe-channel)])
  (:require
    [lamina.time :as t]))

(defn next-msg [ch]
  (-> ch read-channel (wait-for-result 12345)))

(defn next-non-zero-msg [ch]
  (->> (repeatedly #(next-msg ch))
    (drop-while zero?)
    first))

(defn next-non-empty-msg [ch]
  (->> (repeatedly #(next-msg ch))
    (drop-while empty?)
    first))

(defn close-all [& chs]
  (doseq [c chs]
    (close c)))

(defmacro is* [& args]
  `(do
     (is ~@args)
     (print ".")
     (flush)))

(defn run-basic-operator-test [subscribe-fn enqueue-fn post-enqueue-fn]
  (let [sum              (subscribe-fn ".x.y.sum()")
        sum*             (subscribe-fn ".select(a: x.y, b: x).a.sum()")
        filtered-sum*    (subscribe-fn ".where(x.y > 1).x.y.sum()")
        filtered-sum**   (subscribe-fn ".x.where(y = 4).y.sum()")
        filtered-sum***  (subscribe-fn ".x.y.where(_ < 4).sum()")
        avg              (subscribe-fn ".x.y.moving-average(period: 75ms)") ;; todo: add :immediate? flag
        rate             (subscribe-fn ".rate()")
        sum-avg          (subscribe-fn ".x.y.sum(period: 0.1s).moving-average(period: 75000us)")
        lookup           (subscribe-fn ".x.y")
        merge-sum        (subscribe-fn ".merge(.x.y, .x.y).sum()")
        ]

    (try

      (doseq [x (range 1 5)]
        (enqueue-fn {:x {:y x}}))

      (when post-enqueue-fn
        (post-enqueue-fn))

      (is* (= 10 (next-msg sum) (next-msg sum*)))
      (is* (= 20 (next-msg merge-sum)))
      
      (is* (= 9 (next-msg filtered-sum*)))
      (is* (= 4 (next-msg filtered-sum**)))
      (is* (= 6 (next-msg filtered-sum***)))
      (is* (= 4 (next-msg rate)))
      (is* (= 2.5 (next-msg avg)))
      (is* (= 10.0 (next-msg sum-avg)))
      (is* (= (range 1 5) (take 4 (repeatedly #(next-msg lookup)))))

      (finally
        (close-all sum sum* filtered-sum* filtered-sum** filtered-sum*** avg rate sum-avg lookup merge-sum)))))

(defn run-group-by-test [subscribe-fn enqueue-fn post-enqueue-fn]
  (let [foo-grouping       (subscribe-fn ".group-by(foo)")
        foo-rate           (subscribe-fn ".group-by(foo).rate()")
        bar-rate           (subscribe-fn ".group-by(facet: bar).rate()")
        bar-rate*          (subscribe-fn ".select(foo, bar).group-by(bar).rate()")
        bar-rate**         (subscribe-fn ".select(bar).group-by(bar).bar.rate()")
        foo-bar-rate       (subscribe-fn ".group-by(foo).select(bar).group-by(bar).rate()")
        foo-bar-rate*      (subscribe-fn ".group-by([foo bar]).rate()")
        filtered-group-by  (subscribe-fn ".where(foo = 'a').group-by(bar).rate()")
        filtered-group-by* (subscribe-fn ".where(foo ~= 'a').group-by(bar).rate()")
        ;filtered-group-by  (subscribe-fn ".group-by(foo).rate().where(_ > 1)")
        val (fn [foo bar] {:foo foo, :bar bar})]
    
    (try

      (doseq [x (map val [:a :a :b :b :c] [:x :x :z :y :y])]
        (enqueue-fn x))
      
      (when post-enqueue-fn
        (post-enqueue-fn))
    
      (is* (= {:a [:x :x], :b [:z :y], :c [:y]}
             (let [m (next-non-empty-msg foo-grouping)]
               
               (zipmap (keys m) (map #(map :bar %) (vals m))))))
      (is* (= {:a 2, :b 2, :c 1}
             (next-non-empty-msg foo-rate)))
      (is* (= {:x 2, :y 2, :z 1}
             (next-non-empty-msg bar-rate)
             (next-non-empty-msg bar-rate*)
             (next-non-empty-msg bar-rate**)))
      (is* (= {:c {:y 1}, :b {:y 1, :z 1}, :a {:x 2}}
             (next-non-empty-msg foo-bar-rate)))
      (is* (= {[:a :x] 2, [:b :z] 1, [:c :y] 1, [:b :y] 1}
             (next-non-empty-msg foo-bar-rate*)))
      (is* (= {:x 2}
             (next-non-empty-msg filtered-group-by)
             (next-non-empty-msg filtered-group-by*)))

      (finally
        (close-all foo-grouping foo-rate bar-rate bar-rate* bar-rate** foo-bar-rate foo-bar-rate* filtered-group-by filtered-group-by*
          )))))

(defn run-merge-streams-test [subscribe-fn enqueue-fn post-enqueue-fn]
  (let [merged-sum (subscribe-fn ".merge(., &abc).x.sum()")
        zipped-sum (subscribe-fn ".zip(a: .group-by(foo).x.sum(),
                                       b: &abc.group-by(foo).x.sum())")]

    (try

      (doseq [x (range 1 5)]
        (enqueue-fn {:foo :bar, :x x :y (* 2 x)}))

      (when post-enqueue-fn
        (post-enqueue-fn))

      (is* (= 20 (next-msg merged-sum)))

      ;; todo: this is staggered in the split-router case
      #_(is* (= {:a {:bar 10}, :b {:bar 10}} (next-msg zipped-sum) (next-msg zipped-sum)))

      (finally
        (close-all merged-sum zipped-sum)))))

(deftest test-operators
  (let [ch (channel)
        sub #(query-stream % ch :period 100)
        enq #(enqueue ch %)]
    (run-basic-operator-test sub enq nil)
    (close ch))
  (let [ch (channel)
        sub #(query-stream % ch :period 100)
        enq #(enqueue ch %)]
    (run-group-by-test sub enq nil)
    (close ch))
  (println))

(deftest test-non-realtime-router
  (let [q (t/non-realtime-task-queue)
        router (trace-router
                 {:generator (fn [{:strs [pattern]}]
                               (select-probes pattern))
                  :task-queue q
                  :payload :value
                  :timestamp :timestamp})
        sub #(subscribe router (str "abc" %) :period 1e5)
        enq #(trace :abc {:timestamp 0, :value %})]
    (run-basic-operator-test sub enq #(t/advance-until q 2e5))
    (run-group-by-test sub enq #(t/advance-until q 5e5))
    (run-merge-streams-test sub enq #(t/advance-until q 5e5))
    (println)))

(deftest test-local-router
  (let [sub #(subscribe local-trace-router (str "abc" %) :period 100)
        enq #(trace :abc %)]
    (run-basic-operator-test sub enq nil)
    (run-group-by-test sub enq nil)
    (run-merge-streams-test sub enq nil)
    (println)))

(deftest test-split-router
  (let [router (aggregating-trace-router local-trace-router)
        sub #(subscribe router (str "abc" %) :period 100)
        enq #(trace :abc %)]
    (run-basic-operator-test sub enq nil)
    (run-group-by-test sub enq nil)
    (run-merge-streams-test sub enq nil)
    (println)))
