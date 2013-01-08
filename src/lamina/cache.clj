;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.cache
  (:use
    [potemkin :only (defprotocol+)]
    [lamina.core.channel :only (on-closed on-error close siphon)])
  (:require
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

(defprotocol+ ChannelCache
  (get-or-create [cache id on-create]
    "something goes here")
  (ids [cache]
    "something goes here")
  (release [cache id]
    "something goes here")
  (clear [cache]
    "something goes here"))

(defn channel-cache
  "Creates a channel namespace, given a function 'generator' which accepts an id
   and returns a channel. This function is not thread-safe, and may be called
   multiple times for a given id.  The 'on-create' callback in get-or-create is
   thread-safe, however, and should be used when single-call semantics are
   necessary."
  [generator]
  (let [m (ConcurrentHashMap.)]
    (reify ChannelCache
      (get-or-create [this id on-create]
        (let [id* (if (nil? id) ::nil id)]
          (if-let [thunk (.get m id*)]
            @thunk
            (let [thunk (delay (generator id))]
              (if-let [pre-existing (.putIfAbsent m id* thunk)]
                @pre-existing
                (let [ch @thunk]
                  (on-closed ch #(release this id))
                  (on-error ch #(log/error % "error in channel-cache"))
                  (when on-create (on-create ch))
                  ch))))))
      (ids [_]
        (map #(if (= ::nil %) nil %) (keys m)))
      (release [_ id]
        (let [id (if (nil? id) ::nil id)]
          (when-let [thunk (.remove m id)]
            (close @thunk))))
      (clear [this]
        (doseq [k (ids this)]
          (release this k))))))

;;;

(defprotocol+ TopicChannelCache
  (id->topic [_ id]))

(defn topic-channel-cache
  "something goes here"
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe]
    :or {topic->id identity}}]
  (let [active-subscriptions (atom {})
        cache (channel-cache generator)]

    (reify

      TopicChannelCache

      ChannelCache

      (get-or-create [_ topic callback]
        (get-or-create cache topic
          (fn [ch]
            (let [id (topic->id topic)]
              
              (swap! active-subscriptions
                assoc id topic)
              
              (when on-subscribe
                (on-subscribe topic id))

              ;; hook up unsubscription
              (on-closed ch 
                (fn []
                  (when on-unsubscribe
                    (on-unsubscribe id))
                  (swap! active-subscriptions
                    dissoc id)))

              (when callback
                (callback))))))

      (release [_ topic]
        (release cache topic))

      (clear [_]
        (clear cache))

      (ids [_]
        (keys @active-subscriptions))

      (id->topic [_ id]
        (@active-subscriptions id)))))

(defn dependent-topic-channel-cache
  "Like a topic-channel-cache, but allows topics to be decomposed into
   a descriptor containing {:topic, :transform, :cache}, which means topics
   can be defined in terms of another topic."
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe
           cache+topic->topic-descriptor]
    :or {topic->id identity}
    :as options}]

  (let [cache (topic-channel-cache options)]

    (reify

      TopicChannelCache
      ChannelCache
      
      (get-or-create [this topic callback]
        (let [created? (atom false)
              topic-channel (get-or-create cache topic #(reset! created? true))]

          (when @created?

            (when-let [{:keys [topic transform cache]} (cache+topic->topic-descriptor this topic)]
              (let [dependent-channel (get-or-create cache topic nil)]
                (siphon (transform dependent-channel) topic-channel)))

            (when callback (callback)))

          topic-channel))

      (release [_ topic]
        (release cache topic))

      (clear [_]
        (clear cache))

      (ids [_]
        (ids cache))

      (id->topic [_ id]
        (id->topic cache id)))))
