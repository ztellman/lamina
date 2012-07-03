;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.operators
  (:use
    [lamina core]
    [lamina.core threads]
    [clojure test]
    [lamina.test utils])
  (:require
    [lamina.executor :as ex]))

(defmacro dosync* [flag & body]
  `(let [f# (fn [] ~@body)]
     (if ~flag
       (dosync (f#))
       (f#))))

(defmacro task [& body]
  `(delay-invoke 1 (fn [] ~@body)))

(defn async-enqueue 
  [transactional? ch messages]
  (task
    (doseq [m messages]
      (Thread/sleep 25)
      (dosync* transactional?
        (enqueue ch m)))
    (Thread/sleep 25)
    (dosync* transactional?
      (close ch))))

(defn print-all-threads []
  (doseq [[k v] (Thread/getAllStackTraces)]
    (prn (.getName k))
    (doseq [s v]
      (prn s))))

(defn realize-and-print [ch*]
  (let [acc (atom [])]
    (try
      (wait-for-result (ex/task (seq (doall (map #(do (swap! acc conj %) %) (lazy-channel-seq ch*))))) 10000)
      (catch Exception e
        (print-all-threads)
        ;;(prn @acc)
        (throw e)))))

(defn result [f ch]
  (let [ch* (f ch)
        result (if (channel? ch*)
                 ;;(wait-for-result (ex/task (doall (lazy-channel-seq ch*))) 10000)
                 (realize-and-print ch*)
                 (wait-for-result ch* 10000))]
    (when (channel? ch*)
      (is (drained? ch*)))
    result))

(defn tick []
  (print ".")
  (flush))

(def n 1)

(defn assert-equivalence [f f* input]
  (let [expected (f input)
        expected (if (sequential? expected)
                   (seq expected)
                   expected)
        trans-f* #(let [v (dosync (f* %))]
                    (Thread/sleep 100)
                    v)]

    #_(testing "pre-populated non-transactional channel"
      (dotimes [_ n]
        (let [ch (channel* :messages input)]
          (close ch)
          (is (= expected (result f* ch))))
        (tick)))

    #_(testing "async enqueue into non-transactional channel"
      (dotimes [_ n]
        (let [ch (channel)]
          (async-enqueue false ch input)
          (is (= expected (result f* ch))))
        (tick)))

    (testing "pre-populated transactional channel"
      (dotimes [_ n]
        (let [ch (channel* :transactional? true :messages input)]
          (dosync (close ch))
          (is (= expected (result trans-f* ch))))
        (tick)))

    (testing "async enqueue into transactional channel"
      (dotimes [_ n]
        (let [ch (channel* :transactional? true)]
          (async-enqueue true ch input)
          (is (= expected (result trans-f* ch))))
        (tick)))

    true))

;;;

(deftest test-take*
  (are [to-take total-elements]
    (assert-equivalence 
      #(take to-take %)
      #(take* to-take %)
      (range total-elements))

    1 0
    5 5
    10 9
    5 10
    ))

(deftest test-take-while*
  (are [predicate total-elements]
    (assert-equivalence
      #(take-while predicate %)
      #(take-while* predicate %)
      (range total-elements))

    even?              0
    even?              10
    odd?               10
    (constantly true)  10
    (constantly false) 1))

(deftest test-reductions*
  (are [f val s]
    (assert-equivalence
      (if val
        #(reductions f val %)
        #(reductions f %))
      (if val
        #(reductions* f val %)
        #(reductions* f %))
      s)

    + nil (range 10)
    + 1   (range 10)
    conj [] [:a :b :c]
    ))

(deftest test-last*
  (are [s]
    (assert-equivalence #(last %) #(last* %) s)

    (range 3)
    (reverse (range 10))))

(deftest test-reduce*
  (are [f val s]
    (assert-equivalence
      (if val
        #(reduce f val %)
        #(reduce f %))
      (if val
        #(reduce* f val %)
        #(reduce* f %))
      s)

    + nil (range 10)
    + 1   (range 10)
    conj [] [:a :b :c]
    ))

(deftest test-partition*
  (are [n step s]
    (assert-equivalence
      #(partition n step %)
      #(partition* n step %)
      s)

    1 1 (range 10)
    2 1 (range 10)
    5 3 (range 10)
    5 5 (range 4)
    ))

(deftest test-partition-all*
  (are [n step s]
    (assert-equivalence
      #(partition-all n step %)
      #(partition-all* n step %)
      s)

    4 2 (range 10)
    3 1 (range 10)
    10 8 (range 20)
    5 4 (range 10)
    5 5 (range 4)
    ))

;;;

(deftest ^:stress stress-test-partition
  (dotimes* [i 1e5]
    (let [s (seq (range 10))]
      (let [ch (channel)]
        (async-enqueue false ch s)
        (is (= (partition 2 1 s) (lazy-channel-seq (partition* 2 1 ch))))))))

(deftest ^:stress stress-test-partition-all
  (dotimes* [i 1e5]
    (let [s (seq (range 4))]
      (let [ch (channel* :transactional? true)]
        (async-enqueue true ch s)
        (is (= (partition-all 5 5 s)
              (lazy-channel-seq (partition-all* 5 5 ch))))))))

(defn identity-chain [ch]
  (lazy-seq
    (let [ch* (map* identity ch)]
      (cons ch* (identity-chain ch*)))))

(deftest ^:stress stress-test-lazy-channel-seq
  (println "\n----\n test lazy-seq \n---\n")
  (dotimes* [i 1e5]
    (let [s (seq (range 10))]
      (let [ch (channel)]
        (async-enqueue false ch s)
        (is (= s (lazy-channel-seq (nth (identity-chain ch) 10) 10000))))))
  (println "\n----\n test transactional lazy-seq \n---\n")
  (dotimes* [i 1e4]
    (prn i)
    (let [s (seq (range 10))]
      (let [ch (channel* :transactional? true)]
        (async-enqueue true ch s)
        (is (= s (lazy-channel-seq (nth (identity-chain ch) 2) 10000)))))))

;;;

(defn operator-benchmarks [transactional?]
  (let [prefix (when transactional? "transactional ")]
    (bench (str prefix "take* 1000")
      (let [ch (channel* :transactional? transactional? :messages (range 1000))]
        (receive-all
          (dosync* transactional?
            (take* 1000 ch))
          (fn [_]))))
    (let [ch (channel* :transactional? transactional?)]
      (receive-all
        (take-while* (constantly true) ch)
        (fn [_]))
      (bench (str prefix "take-while* true")
        (enqueue ch 1)))
    (let [ch (channel* :transactional? transactional?)]
      (reduce* + 0 ch)
      (bench (str prefix "reduce*")
        (enqueue ch 1)))))

(deftest ^:benchmark benchmarks
  (operator-benchmarks false))

(deftest ^:benchmark transactional-benchmarks
  (operator-benchmarks true))
