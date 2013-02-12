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
    [lamina.time :as time]
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

(defn parse-descriptor [x & {:as options}]
  (let [descriptor (if (string? x)
                     (stringify-keys (p/parse-stream x))
                     x)]
    descriptor))

(defn query-stream
  "something goes here"
  [transform-descriptor ch &
   {:keys [task-queue
           timestamp
           payload
           period
           stream-generator]
    :or {payload identity}
    :as options}]
  (let [stream-generator (if timestamp
                           #(->> %
                              stream-generator
                              (map* identity)
                              (defer-onto-queue task-queue timestamp)
                              (map* payload))
                           stream-generator)
        ch (if timestamp
             (->> ch
               (defer-onto-queue task-queue timestamp)
               (map* payload))
             ch)
        f (if (ifn? transform-descriptor)
            #(transform-descriptor ch)
            #(c/transform-trace-stream
               (apply parse-descriptor transform-descriptor (apply concat options))
               ch))
        f #(with-bindings (merge {}
                            (when stream-generator
                              {#'c/*stream-generator* stream-generator}))
             (f))
        f (if period
            #(time/with-period period (f))
            f)
        f (if task-queue
            #(time/with-task-queue task-queue (f))
            f)]
    (f)))

(defn query-seq
  "something goes here"
  [transform-descriptor s &
   {:keys [timestamp
           payload
           period
           seq-generator]
    :or {payload identity}
    :as options}]
  (assert timestamp)
  (let [q (time/non-realtime-task-queue)
        ch (channel)

        enqueue-next
        (fn enqueue-next [s ch]
          (if-let [x (first s)]
            (time/invoke-at q (timestamp x)
              (with-meta
                (fn []
                  (enqueue ch x)
                  (enqueue-next (rest s) ch))
                {:priority Integer/MAX_VALUE}))
            (time/invoke-at q (inc (time/now q))
              (with-meta
                #(close ch)
                {:priority -1}))))

        ch*
        (query-stream transform-descriptor ch
          :task-queue q
          :period period
          :stream-generator (when seq-generator
                              (fn [descriptor]
                                (let [ch (channel)]
                                  (enqueue-next
                                    (seq-generator descriptor)
                                    ch)
                                  ch))))]

    ;; set up consumption of the incoming seq
    (enqueue-next s ch)

    ;; set up production of the outgoing seq
    (let [advance-until-message
          (fn []
            (let [latch (atom true)
                  result (run-pipeline
                           (read-channel* ch* :on-drained ::drained)
                           (fn [val]
                             (reset! latch false)
                             val))]
              (loop []
                (if (and (time/advance q) @latch)
                  (recur)
                  @result))))]
      (->> (repeatedly advance-until-message)
        (map #(hash-map :timestamp (time/now q) :value %))
        (take-while #(not= ::drained (:value %)))))))

;;;

(import-macro c/def-trace-operator)

(defn trace-router
  "something goes here"
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe
           timestamp
           payload
           task-queue]
    :or {payload identity
         task-queue (time/task-queue)}
    :as options}]

  (let [generator (if timestamp
                    #(->> %
                       generator
                       (map* identity)
                       (defer-onto-queue task-queue timestamp)
                       (map* payload))
                    generator)

        topic-fn (fn [this {:strs [operators name] :as topic}]
                   
                   ;; is it a chain of operators?
                   (when (and (not name) (not (empty? operators)))
                     {:cache this
                      :topic (update-in topic ["operators"] butlast)
                      :transform #(c/transform-trace-stream
                                    (update-in topic ["operators"] (comp list last)) %)}))
        router (cache/router
                 (assoc options
                   :generator generator
                   :cache+topic->topic-descriptor topic-fn))]

    (reify cache/IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe- [this topic args]
        (let [descriptor (apply parse-descriptor topic args)
              options (apply hash-map args)
              period (or (:period options)
                       (get descriptor "period")
                       (time/period))]
          (time/with-task-queue task-queue
            (binding [c/*stream-generator* (or c/*stream-generator* #(cache/subscribe this %))]
              (time/with-period period
                (cache/subscribe router descriptor)))))))))

(def
  ^{:doc "something goes here"}
  local-trace-router
  (trace-router
    {:generator (fn [{:strs [pattern]}]
                  (select-probes pattern))}))

(defn aggregating-trace-router
  "something goes here"
  [endpoint-router]
  (let [router (trace-router
                 {:generator
                  (fn [{:strs [endpoint]}]
                    (cache/subscribe endpoint-router endpoint))})]
    (reify cache/IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe- [this topic args]
        (let [options (apply hash-map args)
              {:strs [operators] :as descriptor} (apply parse-descriptor topic args)
              period (or (options :period)
                       (get descriptor "period")
                       (time/period))
              distributable (assoc descriptor
                              "operators" (c/distributable-chain operators)
                              "period" period)
              non-distributable (assoc descriptor
                                  "endpoint" distributable 
                                  "operators" (c/non-distributable-chain operators))]
          (binding [c/*stream-generator* (or c/*stream-generator* #(cache/subscribe this %))]
            (time/with-period period
              (cache/subscribe router non-distributable))))))))

