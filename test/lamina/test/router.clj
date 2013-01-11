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
    [lamina.core]
    [lamina.cache :only (get-or-create)]
    [lamina.trace :only (trace)]))

(defn next-msg [ch]
  (-> ch read-channel (wait-for-result 10000)))

(defn next-non-zero-msg [ch]
  (->> (repeatedly #(next-msg ch))
    (drop-while zero?)
    first))

(defn close-all [& chs]
  (doseq [c chs]
    (close c)))

(defmacro is* [& args]
  `(do
     (is ~@args)
     (print ".")))

(defn run-basic-operator-test [subscribe-fn enqueue-fn sleep-period]
  (let [sum              (subscribe-fn "x.y.sum()")
        sum*             (subscribe-fn "select(a: x.y, b: x).a.sum()")
        filtered-sum*    (subscribe-fn "where(x.y > 1).x.y.sum()")
        filtered-sum**   (subscribe-fn "x.where(y = 4).y.sum()")
        filtered-sum***  (subscribe-fn "x.y.where(_ < 4).sum()")
        avg              (subscribe-fn "x.y.moving-average(period: 1500)")
        rate             (subscribe-fn "rate(period: 1000)")
        sum-avg          (subscribe-fn "x.y.sum().moving-average(period: 1500)")
        lookup           (subscribe-fn "x.y")]

    (try

      (Thread/sleep sleep-period)
    
      (doseq [x (range 1 5)]
        (enqueue-fn {:x {:y x}}))
    
      (is* (= 10 (next-non-zero-msg sum) (next-non-zero-msg sum*)))
      (is* (= 9 (next-non-zero-msg filtered-sum*)))
      (is* (= 4 (next-non-zero-msg filtered-sum**)))
      (is* (= 6 (next-non-zero-msg filtered-sum***)))
      (is* (= 4 (next-non-zero-msg rate)))
      (is* (= 2.5 (next-non-zero-msg avg)))
      (is* (= 10.0 (next-non-zero-msg sum-avg)))
      (is* (= (range 1 5) (take 4 (repeatedly #(next-msg lookup)))))

      (Thread/sleep 1000)
        
      (doseq [x (range 6 10)]
        (enqueue-fn {:x {:y x}}))
    
      (is* (= 30 (next-non-zero-msg sum)))

      (finally
        (close-all sum sum* filtered-sum* filtered-sum** filtered-sum*** avg rate sum-avg lookup)))))

(defn run-group-by-test [subscribe-fn enqueue-fn sleep-period]
  (let [foo-grouping   (subscribe-fn "group-by(foo).partition-every(1000)")
        foo-rate       (subscribe-fn "group-by(foo).rate()")
        bar-rate       (subscribe-fn "group-by(facet: bar).rate()")
        bar-rate*      (subscribe-fn "select(foo, bar).group-by(bar).rate()")
        bar-rate**     (subscribe-fn "select(bar).group-by(bar).bar.rate()")
        foo-bar-rate   (subscribe-fn "group-by(foo).select(bar).group-by(bar).rate()")
        foo-bar-rate*  (subscribe-fn "group-by([foo bar]).rate()")
        val (fn [foo bar] {:foo foo, :bar bar})]
    
    (try

      (Thread/sleep sleep-period)

      (doseq [x (map val [:a :a :b :b :c] [:x :x :z :y :y])]
        (enqueue-fn x))
    
      (is* (= {:a [:x :x], :b [:z :y], :c [:y]}
            (let [m (next-msg foo-grouping)]
              (zipmap (keys m) (map #(map :bar %) (vals m))))))
      (is* (= {:a 2, :b 2, :c 1}
            (next-msg foo-rate)))
      (is* (= {:x 2, :y 2, :z 1}
            (next-msg bar-rate) (next-msg bar-rate*) (next-msg bar-rate**)))
      (is* (= {:c {:y 1}, :b {:y 1, :z 1}, :a {:x 2}}
            (next-msg foo-bar-rate)))
      (is* (= {[:a :x] 2, [:b :z] 1, [:c :y] 1, [:b :y] 1}
            (next-msg foo-bar-rate*)))

      (finally
        (close-all foo-grouping foo-rate bar-rate bar-rate* bar-rate** foo-bar-rate foo-bar-rate*)))))

(deftest test-operators
  (let [ch (permanent-channel)
        sub #(? % ch)
        enq #(enqueue ch %)]
    (run-basic-operator-test sub enq 0)
    (run-group-by-test sub enq 0)
    (force-close ch)
    (println)))

(deftest test-local-router
  (let [sub #(let [ch (get-or-create local-router (parse-descriptor (str "abc." %)) nil)
                   ch* (channel)]
               (siphon ch ch*)
               ch*)
        enq #(trace :abc %)]
    (run-basic-operator-test sub enq 0)
    (run-group-by-test sub enq 0)
    (println)))
