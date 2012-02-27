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
    [lamina.viz.node :as n]
    [lamina.trace :as trace]))

(defn view-graph
  "Given one or more channels, opens a window displaying those channels and all downstream
   channels."
  [& channels]
  (->> channels
    (map #(vector (c/receiver-node %) (c/emitter-node %)))
    (apply concat)
    distinct
    (apply n/view-graph)))

(defn view-propagation
  "Given a channel and a message, opens a window displaying the value of the message as it is
   propagated downstream.  This is safe to do while other threads are using the channel."
  [channel message]
  (n/trace-message (c/receiver-node channel) message))
