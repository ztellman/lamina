;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.cache
  (:use
    [potemkin :only (definterface+)]
    [lamina.core.channel :only (channel channel* on-closed on-error close siphon)])
  (:require
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

(definterface+ IChannelCache
  (get-or-create [cache id on-create]
    "Gets a channel keyed to `id`.  If `on-create` is non-nil and the entry must be created, 
     it will be called with zero parameters.

     If the channel returned is closed, it will be implicitly removed from the cache.")
  (ids [cache]
    "Returns a list of ids for all channels int he cache.")
  (release [cache id]
    "Removes and closes the channel keyed to `id`.")
  (clear [cache]
    "Removes and closes all channels in the cache."))

(defn channel-cache
  "Creates a channel namespace, given a function `generator` which accepts an id
   and returns a channel. This function is not thread-safe, and may be called
   multiple times for a given id.  The 'on-create' callback in get-or-create is
   thread-safe, however, and should be used when single-call semantics are
   necessary."
  [generator]
  (let [m (ConcurrentHashMap.)]
    (reify lamina.cache.IChannelCache
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

(definterface+ ITopicChannelCache
  (id->topic [_ id] 
	"Returns the `topic` associated with the `id`, which by default are the same.  See `topic-channel-cache`."))

(defn topic-channel-cache
  "A channel cache with some extra functionality to handle the use of the cache for pub-sub mechanisms.

   In addition to a `generator` which is a map of `topic` onto channel, there is a function which transforms
   the `topic` into an `id`.  This can be useful when a non-reversible shorthand for the `topic` is desirable,
   such as a simple incrementing number.  By default, the `topic` and `id` will be the same.

   `on-subscribe` and `on-unsubscribe` are callbacks which will be called with the `cache`, `topic`, and `id`
   whenever a channel is created or closed, respectively."
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe]
    :or {topic->id identity}}]
  (let [active-subscriptions (atom {})
        cache (channel-cache generator)]

    (reify

      lamina.cache.ITopicChannelCache

      lamina.cache.IChannelCache

      (get-or-create [this topic callback]
        (get-or-create cache topic
          (fn [ch]
            (let [id (topic->id topic)]
              
              (swap! active-subscriptions
                assoc id topic)
              
              (when on-subscribe
                (on-subscribe this topic id))

              ;; hook up unsubscription
              (on-closed ch 
                (fn []
                  (when on-unsubscribe
                    (on-unsubscribe this topic id))
                  (swap! active-subscriptions
                    dissoc id)))

              (when callback
                (callback ch))))))

      (release [_ topic]
        (release cache topic))

      (clear [_]
        (clear cache))

      (ids [_]
        (keys @active-subscriptions))

      (id->topic [_ id]
        (@active-subscriptions id)))))

(defn dependent-topic-channel-cache
  "Like `topic-channel-cache`, but allows topics to be defined in terms of each other.

   If `cache+topic->topic-descriptor` is defined, it will be given `cache` and `topic` for each
   channel.  If it returns `nil`, the `generator` will be called for that topic.  If it returns
   a map containing `{:cache, :topic, :transform}`, the topic will be instantiated as a channel
   defined in terms of the given `:topic` composed with the `:transform`."
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe
           cache+topic->topic-descriptor]
    :or {topic->id identity}
    :as options}]

  (let [cache (topic-channel-cache options)]

    (reify

      lamina.cache.ITopicChannelCache
      lamina.cache.IChannelCache
      
      (get-or-create [this topic callback]
        (let [created? (atom false)
              topic-channel (get-or-create cache topic (fn [_] (reset! created? true)))]

          (when @created?

            (when-let [{:keys [topic transform cache]} (cache+topic->topic-descriptor this topic)]
              (let [dependent-channel (get-or-create cache topic nil)]
                (siphon (transform dependent-channel) topic-channel)))

            (when callback (callback topic-channel)))

          topic-channel))

      (release [_ topic]
        (release cache topic))

      (clear [_]
        (clear cache))

      (ids [_]
        (ids cache))

      (id->topic [_ id]
        (id->topic cache id)))))

;;;

(definterface+ IRouter
  (inner-cache [router])
  (subscribe [router topic options] 
	"Returns a channel corresponding to `topic`.  `options` are router-specific, and may be `nil`."))

(defn router
  "Like a cache, except that it doesn't assume exclusive control of the generated channels.
   Rather, bridges are built between the router and the generated channels, which are closed
   when there are no more subscribers.

   Similarly, channels returned by `subscribe` can be closed without affecting other subscribers."
  [{:keys [generator
           topic->id
           on-subscribe
           on-unsubscribe
           cache+topic->topic-descriptor]
    :or {topic->id identity
         cache+topic->topic-descriptor (constantly nil)}
    :as options}]
    (let [bridges (channel-cache (fn [_] (channel* :description "bridge")))
        cache (dependent-topic-channel-cache
                (assoc options
                  :generator
                  #(channel* :description (pr-str %), :grounded? true)
                  
                  :on-subscribe
                  (fn [this topic id]
                    
                   ;; can it escape the router?
                   (when-not (cache+topic->topic-descriptor this topic)
                     (let [bridge (get-or-create bridges topic nil)]
                       (siphon (generator topic) bridge)
                       (siphon bridge (get-or-create this topic nil))))
                    
                    (when on-subscribe
                      (on-subscribe this topic id)))
                  
                  :on-unsubscribe
                  (fn [this topic id]
                    
                    ;; todo: this will create and immediately close a lot of channels, potentially
                    (close (get-or-create bridges topic nil))
                    
                    (when on-unsubscribe
                      (on-unsubscribe this topic id)))
                  
                  :cache+topic->topic-descriptor
                  cache+topic->topic-descriptor))]

      (reify lamina.cache.IRouter
        (inner-cache [_]
          cache)
        (subscribe [_ topic options]
          (let [ch (get-or-create cache topic nil)]
            (siphon ch (channel)))))))
