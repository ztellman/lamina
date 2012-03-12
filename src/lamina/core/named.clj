;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.named
  (:use
    [lamina.core.channel])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

(def ^ConcurrentHashMap named-channels (ConcurrentHashMap.))

(defn named-channel
  "something goes here"
  [id on-create]
  (if-let [ch (.get named-channels id)]
    ch
    (let [ch (channel* :description (pr-str id) :permanent? true)]
      (or (.putIfAbsent named-channels id ch)
        (do
          (on-create ch)
          ch)))))

(defn release-named-channel
  "something goes here"
  [id]
  (.remove named-channels id))
