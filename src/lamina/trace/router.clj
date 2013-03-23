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
     [operators :as o]])
  (:import
    [lamina.cache
     IRouter]))

(defn parse-descriptor [x {:as options}]
  (let [descriptor (if (string? x)
                     (p/parse-stream x)
                     x)]
    descriptor))

(defn query-streams
  "something goes here"
  [descriptor->channel
   {:keys [task-queue
           timestamp
           payload
           period
           stream-generator]
    :or {payload identity}
    :as options}]
  (let [;; make sure inner-streams are properly deferred
        stream-generator (if timestamp
                           #(->> %
                              stream-generator
                              (map* identity)
                              (defer-onto-queue task-queue timestamp)
                              (map* payload))
                           stream-generator)

        ;; make sure input streams are properly deferred
        descriptor->channel (zipmap
                              (keys descriptor->channel)
                              (if timestamp
                                (->> descriptor->channel
                                  vals
                                  (map
                                    #(->> %
                                       (defer-onto-queue task-queue timestamp)
                                       (map* payload))))
                                (vals descriptor->channel)))

        ;; parse and apply descriptors
        f #(zipmap
             (keys descriptor->channel)
             (map
               (fn [[descriptor ch]]
                 (if (ifn? descriptor)
                   (descriptor ch)
                   (let [desc (parse-descriptor descriptor options)
                         ch (or ch (stream-generator (desc :pattern)))]
                     (c/transform-trace-stream desc ch))))
               descriptor->channel))

        ;; set up evaluation scopes
        f #(with-bindings
             (merge {}
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

(defn query-stream
  "something goes here"
  [descriptor
   {:keys [task-queue
           timestamp
           payload
           period
           stream-generator]
    :or {payload identity}
    :as options}
   ch]
  (->> (query-streams {descriptor ch} options)
    vals
    first))

(defn query-seqs
  "something goes here"
  [descriptor->seq
   {:keys [timestamp
           payload
           period
           seq-generator]
    :or {payload identity}
    :as options}]
  (assert timestamp)
  (let [start (comment (->> descriptor->seq
                 vals
                 (map (comp timestamp first))
                 (apply min)))
        q (time/non-realtime-task-queue 0 false)

        ;; lazily iterate over seqs
        enqueue-next
        (fn enqueue-next [s ch]
          (if-let [s (seq s)]
            (let [x (first s)]
              (time/invoke-at q (timestamp x)
                (with-meta
                  (fn []
                    (enqueue ch (payload x))
                    (enqueue-next (rest s) ch))
                  {:priority Integer/MAX_VALUE})))
            (time/invoke-at q (inc (time/now q))
              (with-meta
                #(close ch)
                {:priority -1}))))

        chs (repeatedly (count descriptor->seq) channel)

        ;; create output streams
        descriptor->ch
        (query-streams (zipmap
                         (keys descriptor->seq)
                         (map #(when %1 %2) (vals descriptor->seq) chs))
          {:task-queue q
           :period period
           :stream-generator (when seq-generator
                               (fn [descriptor]
                                 (let [ch (channel)]
                                   (enqueue-next
                                     (seq-generator descriptor)
                                     ch)
                                   ch)))})]

    ;; set up consumption of the incoming seqs
    (doseq [[s ch] (map list (vals descriptor->seq) chs)]
      (enqueue-next s ch))

    ;; set up production of the outgoing seq
    (let [advance-until-message
          (fn [ch]
            (let [latch (atom true)
                  result (run-pipeline
                           (read-channel* ch :on-drained ::drained)
                           (fn [val]
                             (reset! latch false)
                             val))]
              (loop []
                (if (and (time/advance q) @latch)
                  (recur)
                  @result))))]

      (zipmap
        (keys descriptor->ch)
        (map
          (fn [ch]
            (->> (repeatedly #(advance-until-message ch))
              (take-while #(not= ::drained %))
              (map #(hash-map :timestamp (time/now q) :value %))))
          (vals descriptor->ch))))))

(defn query-seq
  "something goes here"
  [descriptor
   {:keys [timestamp
           payload
           period
           seq-generator]
    :or {payload identity}
    :as options}
   s]
  (->> (query-seqs {descriptor s} options)
    vals
    first))

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

        topic-fn (fn [this {:keys [operators name] :as topic}]
                   
                   ;; is it a chain of operators?
                   (when (and (not name) (not (empty? operators)))
                     {:cache this
                      :topic (update-in topic [:operators] butlast)
                      :transform #(c/transform-trace-stream
                                    (update-in topic [:operators] (comp list last)) %)}))
        router (cache/router
                 (assoc options
                   :generator generator
                   :cache+topic->topic-descriptor topic-fn))]

    (reify IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe [this topic options]
        (let [descriptor (parse-descriptor topic options)
              period (or (:period options)
                       (:period descriptor)
                       (time/period))]
          (time/with-task-queue task-queue
            (binding [c/*stream-generator* (or c/*stream-generator*
                                             #(cache/subscribe this % options))]
              (time/with-period period
                (cache/subscribe router descriptor options)))))))))

(def
  ^{:doc "something goes here"}
  local-trace-router
  (trace-router
    {:generator (fn [{:keys [pattern]}]
                  (select-probes pattern))}))

(defn aggregating-trace-router
  "something goes here"
  [endpoint-router]
  (let [router (trace-router
                 {:generator
                  (fn [{:keys [endpoint]}]
                    (cache/subscribe endpoint-router endpoint {}))})]
    (reify IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe [this topic options]
        (let [{:keys [operators] :as descriptor} (parse-descriptor topic options)
              period (or (:period options)
                       (:period descriptor)
                       (time/period))
              distributable (assoc descriptor
                              :operators (c/distributable-chain operators)
                              :period period)
              non-distributable (assoc descriptor
                                  :endpoint distributable 
                                  :operators (c/non-distributable-chain operators))]
          (binding [c/*stream-generator* (or c/*stream-generator* #(cache/subscribe this % options))]
            (time/with-period period
              (cache/subscribe router non-distributable options))))))))

