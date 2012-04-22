;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.graph.core
  (:use 
    [lamina.core.utils]
    [potemkin]))

;;;

(deftype-once Edge [^String description node]
  IDescribed
  (description [_] description))

(defn edge [description node]
  (Edge. description node))

(defn edge? [x]
  (instance? Edge x))

;;;

(defprotocol-once IPropagator
  (downstream [_]
    "Returns a list of nodes which are downstream of this node.")
  (transactional [_]
    "Makes this node and all downstream nodes transactional.")
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed.")
  (close [_])
  (error [_ err]))

(defn downstream-nodes [n]
  (map #(.node ^Edge %) (downstream n)))

;;;
