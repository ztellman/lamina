(ns lamina.test.manifold
  (:use
    [clojure test]
    [lamina core])
  (:require
    [manifold.stream :as s]
    [manifold.deferred :as d]))

(defn validate-put-take [in out]
  (let [d (s/put! in 1)]
    (is (= 1 @(s/take! out)))
    (is (= true @d))))

(deftest test-manifold-interop
  (let [c (channel)
        in (s/->sink c)
        out (s/->source c)]

    (validate-put-take in out)

    (let [c' (channel)
          out (s/->source c')]
      (s/connect c c')
      (validate-put-take in out))))
