;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.channel
  (:use
    [clojure test]
    [lamina core cache viz]))

(deftest test-channel-cache
  (let [c (channel-cache (fn [_] (channel)))]
    (let [created? (atom false)
          ch (get-or-create c :foo (fn [_] (reset! created? true)))]
      (is @created?)
      (let [created? (atom false)]
        (is (= ch (get-or-create c :foo (fn [_] (reset! created? true)))))
        (is (= false @created?)))
      
      (is (not= ch (get-or-create c :bar nil)))

      (close ch)
      (let [created? (atom false)]
        (is (not= ch (get-or-create c :foo (fn [_] (reset! created? true)))))
        (is @created?)))))

(deftest test-topic-channel-cache
  (let [subscriptions (channel)
        unsubscriptions (channel)
        c (topic-channel-cache
            {:generator (fn [_] (channel))
             :topic->id #(str "id:" (name %))
             :on-subscribe (fn [& args] (enqueue subscriptions args))
             :on-unsubscribe (fn [& args] (enqueue unsubscriptions args))})]

    (let [created? (atom false)
          ch (get-or-create c :foo (fn [_] (reset! created? true)))]
      (is @created?)
      (is (= :foo (id->topic c "id:foo")))
      (is (= [c :foo "id:foo"] @(read-channel subscriptions)))
      (close ch)
      (is (= [c :foo "id:foo"] @(read-channel unsubscriptions))))))

(deftest test-dependent-topic-channel-cache
  (let [c (dependent-topic-channel-cache
            {:generator #(channel* :description %)
             :cache+topic->topic-descriptor (fn [cache topic]
                                              (when-not (empty? topic)
                                                {:cache cache
                                                 :topic (vec (rest topic))
                                                 :transform #(map* (partial + (first topic)) %)}))})
        base-channel (get-or-create c [] nil)
        a (get-or-create c [3 2 1] nil)
        b (get-or-create c [4 2 1] nil)]
    (enqueue base-channel 0)
    (let [c (get-or-create c [4 3 2 1] nil)]

      ;; the message has been consumed by c
      (is (= ::none @(read-channel* a :timeout 0 :on-timeout ::none)))
      
      (is (= 10 @(read-channel c)))
      (is (= 7 @(read-channel b))))))
