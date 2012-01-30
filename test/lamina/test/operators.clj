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
    (dosync* transactional? (close ch))))

(defn result [f ch]
  (let [ch* (f ch)
        result (if (channel? ch*)
                 (doall (lazy-channel-seq ch* 5000))
                 @ch*)]
    (when (channel? ch*)
      (is (drained? ch*)))
    result))

(defn assert-equivalence [f f* input]
  (let [expected (f input)
        expected (if (sequential? expected)
                   (seq expected)
                   expected)
        trans-f* #(dosync (f* %))]

    ;; pre-populated non-transactional channel
    (let [ch (channel* :messages input)]
      (close ch)
      (is (= expected (result f* ch))))

    ;; async enqueue into non-transactional channel
    (let [ch (channel)]
      (async-enqueue false ch input)
      (is (= expected (result f* ch))))

    ;; pre-populated transactional channel
    (let [ch (channel* :transactional? true :messages input)]
      (dosync (close ch))
      (is (= expected (result trans-f* ch))))

    ;; async enqueue into transactional channel
    (let [ch (channel* :transactional? true)]
      (async-enqueue true ch input)
      (is (= expected (result trans-f* ch))))))

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

;;;

(deftest ^:stress stress-test-lazy-channel-seq
  (println "\n----\n test lazy-seq \n---\n")
  (dotimes* [i 1e5]
    (let [s (seq (range 0))]
      (let [ch (channel)]
        (async-enqueue false ch s)
        (is (= s (lazy-channel-seq ch 10000))))))
  (println "\n----\n test transactional lazy-seq \n---\n")
  (dotimes* [i 1e5]
    (let [s (seq (range 0))]
      (let [ch (channel* :transactional? true)]
        (async-enqueue true ch s)
        (is (= s (lazy-channel-seq ch 10000)))))))

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
