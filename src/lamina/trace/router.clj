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
    [lamina.query :as q]
    [lamina.query.core :as c])
  (:import
    [lamina.cache
     IRouter]))

;;;

(defn trace-router
  "Creates a router which can be queried via the default syntax and allows multiple queries to share sub-streams.

   Like `lamina.cache/router`, with the addition of:
	
   `:timestamp` - a function which returns the logical time of each message, defaults to wall time.
  
   `:payload` - a function which describes the content of each message, defaults to `identity`.
  "
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe
           timestamp
           payload
           task-queue
           auto-advance?]
    :or {payload identity
         task-queue (time/task-queue)
         auto-advance? false}
    :as options}]

  (let [generator (if timestamp
                    #(->> %
                       generator
                       (map* identity)
                       (defer-onto-queue
                         {:task-queue task-queue
                          :timestamp timestamp
                          :auto-advance? auto-advance?})
                       (map* payload))
                    generator)

        topic-fn (fn [this {:keys [operators name] :as topic}]
                   
                   ;; is it a chain of operators?
                   (when (and (not name) (not (empty? operators)))
                     {:cache this
                      :topic (update-in topic [:operators] butlast)
                      :transform #(c/transform-stream
                                    (update-in topic [:operators] (comp list last)) %)}))
        router (cache/router
                 (assoc options
                   :generator generator
                   :cache+topic->topic-descriptor topic-fn))]

    (reify IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe [this topic options]
        (let [descriptor (q/parse-descriptor topic options)
              period (or (:period options)
                       (:period descriptor)
                       (time/period))]
          (time/with-task-queue task-queue
            (binding [c/*stream-generator* (or c/*stream-generator*
                                             #(cache/subscribe this % options))]
              (time/with-period period
                (cache/subscribe router descriptor options)))))))))

(def
  ^{:doc "A trace-router which can be used to consume and analyze probe-channels."}
  local-trace-router
  (trace-router
    {:generator (fn [{:keys [pattern]}]
                  (select-probes pattern))}))

(defn aggregating-trace-router
  "A trace-router which splits the query into distributable and non-distributable portions, and forwards
   the distributable portion to `endpoint-router`."
  [endpoint-router]
  (let [router (trace-router
                 {:generator
                  (fn [{:keys [endpoint]}]
                    (cache/subscribe endpoint-router endpoint {}))})]
    (reify IRouter
      (inner-cache [_]
        (cache/inner-cache router))
      (subscribe [this topic options]
        (let [{:keys [operators] :as descriptor} (q/parse-descriptor topic options)
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

