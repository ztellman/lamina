;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.probe
  (:require
    [lamina.core.channel :as c]
    [lamina.core.node :as n])
  (:import
    [java.util.concurrent
     ConcurrentHashMap]))

(defprotocol IProbe
  (enabled? [_]))

(deftype ProbeChannel
  [^AtomicBoolean enabled?
   channel]
  IProbe
  (enabled? []
    (.get enabled?))
  c/IChannel
  (receiver-node [_]
    (c/receiver-node channel))
  (emitter-node [_]
    (c/emitter-node channel)))

(defn probe-channel- []
  (let [flag (AtomicBoolean. false)
        ch (c/channel* :probe? true)]

    ;; set the flag whenever the downstream count changes
    (n/on-state-changed (c/emitter-node ch)
      (fn [_ downstream _]
        (.set flag (pos? downstream))))

    (ProbeChannel. flag ch)))

(def ^ConcurrentHashMap probes (ConcurrentHashMap.))

(defn probe-channel [id]
  (if-let [ch (.get probes id)]
    ch
    (let [ch (probe-channel-)]
      (or (.putIfAbsent probes id ch) ch))))

;;;

(deftype SympatheticProbeChannel
  [^AtomicBoolean enabled?
   upstream
   downstream]
  IProbe
  (enabled? [_]
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
    (n/on-state-changed (c/emitter-node ch)
      (fn [_ downstream _]
        (if (zero? downstream)
          (when (.compareAndSet enabled? true false)
            (n/link receiver emitter emitter nil))
          (when (.compareAndSet enabled? false true)
            (n/cancel receiver emitter)))))

    (SympatheticProbeChannel. enabled? upstream downstream)))
