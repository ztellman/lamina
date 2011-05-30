;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.trace
  (:use
    [lamina core trace]
    [clojure test]))

(defn clear-trace-channels []
  (dosync
    (ref-set trace-channels {})
    (ref-set enabled-trace-channels #{})))


(deftest test-trace
  (clear-trace-channels)
  (let [marker (atom false)]
    (trace :trace (reset! marker true))
    (is (not @marker))
    (receive-all (trace-channel :trace) (fn [_] ))
    (trace :trace (reset! marker true))
    (is @marker)))

(deftest test-trace->>
  (clear-trace-channels)
  (let [marker (atom nil)
	ch (channel)]
    (receive-all ch #(reset! marker %))
    (enqueue
      (trace->> :trace (map* inc) [ch])
      1)
    (is (= nil @marker)))
  (let [marker (atom nil)
	trace-marker (atom nil)
	ch (channel)]
    (receive-all ch #(reset! marker %))
    (receive-all (trace-channel :trace) #(reset! trace-marker %))
    (enqueue
      (trace->> :trace (map* inc) [ch])
      1)
    (is (= 2 @marker))
    (is (= 2 @trace-marker)))
  (let [a-marker (atom nil)
	a-b-marker (atom nil)]
    (receive-all (trace-channel :a) #(reset! a-marker %))
    (receive-all (trace-channel :a.b) #(reset! a-b-marker %))
    (enqueue
      (trace->> :a (map* inc) [(trace->> :b (map* inc))])
      1)
    (is (= 2 @a-marker))
    (is (= 3 @a-b-marker))))


