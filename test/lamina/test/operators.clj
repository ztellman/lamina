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
    [lamina.test utils]))

(defmacro dosync* [flag & body]
  `(let [f# (fn [] ~@body)]
     (if ~flag
       (dosync (f#))
       (f#))))

(defmacro task [& body]
  `(delay-invoke 1 (fn [] ~@body)))

(defn async-enqueue [transactional? ch messages]
  (task
    (doseq [m messages]
      (Thread/yield)
      (dosync* transactional?
        (enqueue ch m)))
    (close ch)))

(defn result [f ch]
  (let [ch* (f ch)
        result (doall (lazy-channel-seq ch* 2000))
        ]
    (is (drained? ch*))
    result))

(defn assert-transactional-equivalence [f f* input]
  (let [expected (seq (f input))
        f* #(dosync (f* %))]
    (let [ch (channel* :transactional? true :messages input)]
      (close ch)
      (is (= expected (result f* ch))))
    (let [ch (channel* :transactional? true)]
      (async-enqueue true ch input)
      (is (= expected (result f* ch))))))

(defn assert-equivalence [f f* input]
  (let [expected (seq (f input))]
    (let [ch (channel* :messages input)]
      (close ch)
      (is (= expected (result f* ch))))
    (let [ch (channel)]
      (async-enqueue false ch input)
      (is (= expected (result f* ch))))))

(defn assert-full-equivalence [f f* input]
  (assert-transactional-equivalence f f* input)
  (assert-equivalence f f* input))

;;;

(deftest test-take*
  (are [to-take total-elements]
    (assert-full-equivalence #(take to-take %) #(take* to-take %) (range total-elements))

    1 0
    5 5
    10 9
    5 10
    ))

(deftest test-take-while*
  (are [predicate total-elements]
    (assert-full-equivalence #(take-while predicate %) #(take-while* predicate %) (range total-elements))

    even?              0
    even?              10
    odd?               10
    (constantly true)  10
    (constantly false) 1))

;;;

(deftest ^:stress stress-test-lazy-channel-seq
  (dotimes* [i 1e4]
    (let [s (seq (range 0))]
      (let [ch (channel)]
        (async-enqueue false ch s)
        (is (= s (lazy-channel-seq ch 2500)))))))

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
        (dosync* transactional?
          (take-while* (constantly true) ch))
        (fn [_]))
      (bench (str prefix "take-while* true")
        (enqueue ch 1)))))

(deftest ^:benchmark benchmarks
  (operator-benchmarks false))

(deftest ^:benchmark transactional-benchmarks
  (operator-benchmarks true))
