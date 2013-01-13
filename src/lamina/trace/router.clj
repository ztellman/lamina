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

(defn ? [transform-descriptor ch]
  (c/transform-trace-stream (parse-descriptor (str "." transform-descriptor)) ch))

;;;

(defprotocol+ IRouter
  (subscribe- [_ descriptor args]))

(defn subscribe [router topic & args]
  (subscribe- router topic args))

(defn trace-router
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe]
    :or {topic->id identity}
    :as options}]

  (let [bridges (cache/channel-cache (fn [_] (channel* :description "bridge")))
        cache (cache/dependent-topic-channel-cache
                (assoc options
                  :generator
                  #(channel* :description %)
                  
                  :on-subscribe
                  (fn [this topic id]
                    
                    (let [{:strs [name operators]} topic]
                      
                      ;; can it escape the router?
                      (when (and (not name) (empty? operators))
                        (siphon
                          (generator topic)
                          (cache/get-or-create bridges topic nil)
                          (cache/get-or-create this topic nil))))
                    
                    (when on-subscribe
                      (on-subscribe this topic id)))
                  
                  :on-unsubscribe
                  (fn [this topic id]
                    
                    ;; todo: this will create and immediately close a lot of channels, potentially
                    (close (cache/get-or-create bridges topic nil))
                    
                    (when on-unsubscribe
                      (on-unsubscribe this topic id)))
                  
                  :cache+topic->topic-descriptor
                  (fn [this {:strs [operators name] :as topic}]
                    
                    ;; is it a chain of operators?
                    (when (and (not name) (not (empty? operators)))
                      {:cache this
                       :topic (update-in topic ["operators"] butlast)
                       :transform #(c/transform-trace-stream
                                     (update-in topic ["operators"] (partial take-last 1)) %)}))))]

    (reify IRouter
      (subscribe- [_ topic args]
        (let [ch (cache/get-or-create cache (parse-descriptor topic) nil)]
          (siphon ch (channel)))))))

(def local-router
  (trace-router
    {:generator (fn [{:strs [pattern]}]
                  (select-probes pattern))}))

