;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.router
  (:use
    [potemkin]
    [lamina.core]
    [clojure.walk :only (postwalk)]
    [lamina.trace.probe :only (select-probes)])
  (:require
    [lamina.cache :as cache]
    [lamina.trace.router
     [core :as c]
     [parse :as p]
     [operators :as o]]))

(defn- stringify-keys [x]
  (postwalk
    (fn [x]
      (if (map? x)
        (zipmap
          (map #(if (keyword? %) (name %) (str %)) (keys x))
          (vals x))
        x))
    x))

(defn parse-descriptor [x]
  (if (string? x)
    (stringify-keys (p/parse-stream x))
    x))

(defn query-stream [transform-descriptor ch]
  (c/transform-trace-stream (parse-descriptor (str "." transform-descriptor)) ch))

;;;

(defn trace-router
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe]
    :as options}]

  (let [options (assoc options
                  :cache+topic->topic-descriptor
                  (fn [this {:strs [operators name] :as topic}]
                    
                    ;; is it a chain of operators?
                    (when (and (not name) (not (empty? operators)))
                      {:cache this
                       :topic (update-in topic ["operators"] butlast)
                       :transform #(c/transform-trace-stream
                                     (update-in topic ["operators"] (partial take-last 1)) %)})))
        router (cache/router options)]

    (reify cache/IRouter
      (subscribe- [_ topic args]
        (cache/subscribe router (parse-descriptor topic) nil)))))

(def local-trace-router
  (trace-router
    {:generator (fn [{:strs [pattern]}]
                  (select-probes pattern))}))

(defn aggregating-trace-router [endpoint-router]
  (let [router (trace-router
                 {:generator
                  (fn [{:strs [endpoint]}]
                    (cache/subscribe endpoint-router endpoint))})]
    (reify cache/IRouter
      (subscribe- [_ topic args]
        (let [{:strs [operators] :as topic} (parse-descriptor topic)
              distributable (assoc topic
                              "operators" (c/distributable-chain operators))
              non-distributable (assoc topic
                                  "endpoint" distributable
                                  "operators" (c/non-distributable-chain operators))]
          (cache/subscribe router non-distributable))))))

