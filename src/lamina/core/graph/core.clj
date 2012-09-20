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

(deftype-once Edge [^String description next ^boolean sneaky?]
  IDescribed
  (description [_] description))

(defn edge
  ([description next]
     (edge description next false))
  ([description next sneaky?]
     (Edge. description next sneaky?)))

(defn edge? [x]
  (instance? Edge x))

(defn sneaky-edge? [^Edge e]
  (.sneaky? e))

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

(defn downstream-propagators [n]
  (map #(.next ^Edge %) (downstream n)))

;;;
