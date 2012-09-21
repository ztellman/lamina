;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.cache
  (:use
    [lamina.core.channel :only (on-closed on-error close)])
  (:require
    [clojure.tools.logging :as log])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

(defprotocol ChannelCache
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
              (or

                ;; someone beat us to the punch
                (when-let [pre-existing (.putIfAbsent m id* thunk)]
                  @pre-existing)

                ;; we got in, now initialize
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
