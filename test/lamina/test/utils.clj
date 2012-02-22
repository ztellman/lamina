;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.utils)

(if (and (= 1 (:major *clojure-version*)) (= 2 (:minor *clojure-version*)))
  (defmacro bench [& _])
  (do
    (require '[criterium.core])
    (defmacro bench [name & body]
      `(do
         (println "\n-----\n" ~name "\n-----\n")
         (criterium.core/quick-bench
           (do ~@body)
           :reduce-with #(and %1 %2))))))

(defmacro dotimes* [[i n] & body]
  (let [display-interval (max 1000 (/ n 100))]
    `(let [time# (atom (System/nanoTime))]
       (dotimes [~i ~n]
         (when (and (pos? ~i) (zero? (rem ~i ~display-interval)))
           (let [new-time# (System/nanoTime)]
             (prn ~i (/ (- new-time# @time#) 1e6))
             (reset! time# new-time#)))
         (do ~@body)))))
