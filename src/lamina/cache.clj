;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.cache
  (:use
    [lamina.core.channel :only (on-closed close)])
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
        (if-let [ch (.get m id)]
          ch
          (let [ch (generator id)]
            (or (.putIfAbsent m id ch)
              (do
                (on-closed ch #(release this id))
                (when on-create (on-create ch))
                ch)))))
      (ids [_]
        (keys m))
      (release [_ id]
        (when-let [ch (.remove m id)]
          (close ch)))
      (clear [this]
        (doseq [k (ids this)]
          (close (release this k)))))))
