;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.probe
  (:use
    [lamina.core.utils])
  (:require
    [clojure.string :as str]
    [lamina.core.channel :as c]
    [lamina.core.graph :as g]
    [lamina.core.result :as r]
    [clojure.tools.logging :as log])
  (:import
    [java.util.regex
     Pattern]
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
    (let [result (g/propagate (c/receiver-node channel) msg true)]
      (when (and log-on-disabled? (identical? :lamina/grounded result))
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
        ch (c/channel*
             :grounded? true
             :permanent? true
             :description (name description))]

    ;; set the flag whenever the downstream count changes
    (g/on-state-changed (c/emitter-node ch) nil
      (fn [_ downstream _]
        (.set flag (pos? downstream))))

    (ProbeChannel. flag ch log-on-disabled? description)))

(defn canonical-probe-name [x]
  (cond
    (string? x) x
    (keyword? x) (name x)
    (sequential? x) (->> x (map name) (interpose ":") (apply str))))

(def ^ConcurrentHashMap probes (ConcurrentHashMap.))
(def new-probe-broadcaster (c/channel* :grounded? true, :permanent? true))

(defn probe-channel-generator [f]
  (fn [id]
    (let [id (canonical-probe-name id)]
      (if-let [ch (.get probes id)]
        ch
        (let [ch (f id)]
          (if-let [existing-probe (.putIfAbsent probes id ch)]
            existing-probe
            (do
              (enqueue new-probe-broadcaster id)
              ch)))))))

(def probe-channel (probe-channel-generator #(probe-channel- % false)))

(def error-probe-channel (probe-channel-generator #(probe-channel- % true)))

(defn select-probes [wildcard-string-or-regex]
  (let [ch (c/channel)
        regex (if (string? wildcard-string-or-regex)
                (-> wildcard-string-or-regex (str/replace "*" ".*") Pattern/compile)
                wildcard-string-or-regex)
        callback (fn [id]
                   (when (re-matches regex id)
                     (c/siphon (probe-channel id) ch)))]
    (c/receive-all new-probe-broadcaster callback)
    (c/on-closed ch #(c/cancel-callback new-probe-broadcaster callback))
    (doseq [id (keys probes)]
      (callback id))
    ch))

;;;

(defn probe-result [result]
  (when-not (r/result-channel? result)
    (throw (IllegalArgumentException. "probe-result must be given a result-channel")))
  (reify
    IEnqueue
    (enqueue [_ msg]
      (enqueue result msg))
    IProbe
    (probe-enabled? [_]
      (not (r/result result)))))

;;;

(deftype SympatheticProbeChannel
  [^AtomicBoolean enabled?
   receiver
   emitter]
  IEnqueue
  (enqueue [_ msg]
    (g/propagate (c/receiver-node receiver) msg true))
  IProbe
  (probe-enabled? [_]
    (.get enabled?))
  c/IChannel
  (receiver-node [_]
    (c/receiver-node receiver))
  (emitter-node [_]
    emitter)
  (split-receiver [_]
    (c/split-receiver emitter))
  (toString [_]
    (str emitter)))

(defn sympathetic-probe-channel
  "A channel that only lets messages through if 'ch' has downstream nodes."
  [ch]
  (let [receiver-channel (c/channel* :grounded? true)
        receiver (c/receiver-node receiver-channel)
        emitter (g/node* :permanent? true)
        enabled? (AtomicBoolean. false)]

    ;; bridge the upstream and downstream nodes whenever the source channel is active
    (g/on-state-changed (c/emitter-node ch) nil
      (fn [_ downstream _]
        (if (zero? downstream)
          (when (.compareAndSet enabled? true false)
            (g/link receiver emitter emitter nil nil))
          (when (.compareAndSet enabled? false true)
            (g/cancel receiver emitter)))))

    (SympatheticProbeChannel. enabled? receiver-channel emitter)))

;;;

(defmethod print-method ProbeChannel [o ^Writer w]
  (.write w (str o)))

(defmethod print-method SympatheticProbeChannel [o ^Writer w]
  (.write w (str o)))
