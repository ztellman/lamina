;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.probe
  (:use
    [lamina.core.utils])
  (:require
    [lamina.core.channel :as c]
    [lamina.core.node :as n]
    [clojure.tools.logging :as log])
  (:import
    [java.io
     Writer]
    [java.util.concurrent
     ConcurrentHashMap]
    [java.util.concurrent.atomic
     AtomicBoolean]))

(defprotocol IProbe
  (probe-enabled? [_]))

(deftype ProbeChannel
  [^AtomicBoolean enabled?
   channel
   log-on-disabled?
   description]
  IEnqueue
  (enqueue [_ msg]
    (let [result (n/propagate (c/receiver-node channel) msg true)]
      (when (and log-on-disabled? (identical? :lamina/inactive-probe result))
        (log/error msg (str "error on inactive probe: " description)))
      result))
  IProbe
  (probe-enabled? [_]
    (.get enabled?))
  c/IChannel
  (receiver-node [_]
    (c/receiver-node channel))
  (emitter-node [_]
    (c/emitter-node channel)))

(defn probe-channel- [description log-on-disabled?]
  (let [flag (AtomicBoolean. false)
        ch (c/channel* :probe? true :description (name description))]

    ;; set the flag whenever the downstream count changes
    (n/on-state-changed (c/emitter-node ch) nil
      (fn [_ downstream _]
        (.set flag (pos? downstream))))

    (ProbeChannel. flag ch log-on-disabled? description)))

(defn canonical-probe-name [x]
  (cond
    (string? x) x
    (keyword? x) (name x)
    (sequential? x) (->> x (map name) (interpose ":") (apply str))))

(def ^ConcurrentHashMap probes (ConcurrentHashMap.))

(defn probe-channel-generator [f]
  (fn [id]
    (let [id (canonical-probe-name id)]
      (if-let [ch (.get probes id)]
        ch
        (let [ch (f id)]
          (or (.putIfAbsent probes id ch) ch))))))

(def probe-channel (probe-channel-generator #(probe-channel- % false)))

(def error-probe-channel (probe-channel-generator #(probe-channel- % true)))

;;;

(deftype SympatheticProbeChannel
  [^AtomicBoolean enabled?
   upstream
   downstream]
  IEnqueue
  (enqueue [_ msg]
    (n/propagate (c/receiver-node upstream) msg true))
  IProbe
  (probe-enabled? [_]
    (.get enabled?))
  c/IChannel
  (receiver-node [_]
    (c/receiver-node upstream))
  (emitter-node [_]
    downstream)
  (split-receiver [_]
    (c/split-receiver upstream))
  (toString [_]
    (str upstream)))

(defn sympathetic-probe-channel
  "A channel that only lets messages through if 'ch' has downstream nodes."
  [ch]
  (let [upstream (c/channel* :probe? true)
        downstream (n/node* :permanent? true)
        enabled? (AtomicBoolean. false)]

    ;; bridge the upstream and downstream nodes whenever the source channel is active
    (n/on-state-changed (c/emitter-node ch) nil
      (fn [_ downstream _]
        (if (zero? downstream)
          (when (.compareAndSet enabled? true false)
            (n/link upstream downstream downstream nil nil))
          (when (.compareAndSet enabled? false true)
            (n/cancel upstream downstream)))))

    (SympatheticProbeChannel. enabled? upstream downstream)))

;;;

(defmethod print-method ProbeChannel [o ^Writer w]
  (.write w (str o)))

(defmethod print-method SympatheticProbeChannel [o ^Writer w]
  (.write w (str o)))
