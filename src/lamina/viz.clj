;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.viz
  (:require
    [lamina.core.channel :as c]
    [lamina.viz.core :as core]
    [lamina.viz.graph :as g]
    [lamina.trace :as trace]))

(defn render-graph
  "Given one or more channels, renders them and all downstream nodes."
  [& options+channels]
  (let [options (when (map? (first options+channels))
                  (first options+channels))
        channels (if options
                   (rest options+channels)
                   options+channels)]
    (->> channels
      (map #(vector (c/receiver-node %) (c/emitter-node %)))
      (apply concat)
      distinct
      (apply g/render-graph options))))

(defn view-graph
  "Given one or more channels, displays them and all downstream nodes."
  [& options+channels]
  (core/view-image g/node-frame (apply render-graph options+channels)))

(defn render-propagation
  "Given a channel and a message, renders a graph displaying the value of the message as
   it is propagated downstream.  This is safe to do while other threads are using the
   channel."
  ([channel message]
     (render-propagation nil channel message))
  ([options channel message]
     (g/render-propagation options (c/receiver-node channel) message)))

(defn view-propagation
  "Given a channel and a message, displays a graph displaying the value of the message as
   it is propagated downstream.  This is safe to do while other threads are using the
   channel."
  ([channel message]
     (view-propagation nil channel message))
  ([options channel message]
     (core/view-image g/node-frame (render-propagation options channel message))))
