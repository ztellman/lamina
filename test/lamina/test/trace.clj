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
    [lamina.trace.core :only (probe-channels probe-switches)]
    [clojure test]))

(defn clear-probe-channels []
  (dosync
    (ref-set probe-channels {})
    (reset! probe-switches {})))

(deftest test-trace
  (clear-probe-channels)
  (let [marker (atom false)
	probe :foo]
    (trace probe (reset! marker true))
    (is (not @marker))
    (receive-all (probe-channel probe) (fn [_] ))
    (trace probe (reset! marker true))
    (is @marker))
  (eval
    `(let [marker# (atom false)]
       (trace :bar (reset! marker# true))
       (is (not @marker#))
       (receive-all (probe-channel :bar) (fn [_#] ))
       (trace :bar (reset! marker# true))
       (is @marker#))))

(deftest test-trace->>
  (clear-probe-channels)
  (let [marker (atom nil)
	ch (channel)]
    (receive-all ch #(reset! marker %))
    (enqueue
      (trace->> :trace (map* inc) [ch])
      1)
    (is (nil? @marker)))
  (let [marker (atom nil)
	trace-marker (atom nil)
	ch (channel)]
    (receive-all ch #(reset! marker %))
    (receive-all (probe-channel :trace) #(reset! trace-marker %))
    (enqueue
      (trace->> :trace (map* inc) [ch])
      1)
    (is (= 2 @marker))
    (is (= 2 @trace-marker)))
  (let [a-marker (atom nil)
	a-b-marker (atom nil)]
    (receive-all (probe-channel :a) #(reset! a-marker %))
    (receive-all (probe-channel :a:b) #(reset! a-b-marker %))
    (enqueue
      (trace->> :a (map* inc) [(trace->> :b (map* inc))])
      1)
    (is (= 2 @a-marker))
    (is (= 3 @a-b-marker))))


