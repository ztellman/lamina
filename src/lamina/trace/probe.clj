;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.probe
  (:use
    [potemkin]
    [lamina.core.utils])
  (:require
    [clojure.string :as str]
    [lamina.core.channel :as c]
    [lamina.core.graph :as g]
    [lamina.core.result :as r]
    [clojure.tools.logging :as log])
  (:import
    [java.io
     Writer]
    [java.util.concurrent
     ConcurrentHashMap
     CopyOnWriteArrayList]
    [java.util.concurrent.atomic
     AtomicBoolean]))

(definterface+ IProbe
  (probe-enabled? [_] "Returns true if the probe has downstream channels.")
  (trigger-callbacks [_ enabled?]))

(deftype+ ProbeChannel
  [^AtomicBoolean enabled?
   ^CopyOnWriteArrayList callbacks
   channel
   log-on-disabled?
   description]
  lamina.core.utils.IEnqueue
  (enqueue [_ msg]
    (let [result (g/propagate (c/receiver-node channel) msg true)]
      (when (and log-on-disabled? (identical? :lamina/grounded result))
        (log/error msg (str "error on inactive probe: " description)))
      result))
  IProbe
  (probe-enabled? [_]
    (.get enabled?))
  (trigger-callbacks [_ enabled?]
    (doseq [c callbacks]
      (try
        (c enabled?)
        (catch Exception e
          (log/error e "Error in on-enabled-changed callback.")))))

  lamina.core.utils.IChannelMarker
  lamina.core.channel.IChannel
  (receiver-node [_]
    (c/receiver-node channel))
  (emitter-node [_]
    (c/emitter-node channel)))

(defn probe-channel- [description log-on-disabled?]
  (let [flag (AtomicBoolean. false)
        ch (c/channel*
             :grounded? true
             :permanent? true
             :description (str "probe: " (name description)))
        p (ProbeChannel.
            flag
            (CopyOnWriteArrayList.)
            ch
            log-on-disabled?
            description)]

    ;; set the flag whenever the downstream count changes
    (g/on-state-changed (c/emitter-node ch) nil
      (fn [_ downstream _]
        (let [enabled? (pos? downstream)]
          (when-not (= enabled? (.getAndSet flag enabled?))
            (trigger-callbacks p enabled?)))))

    p))

(defn on-enabled-changed [^ProbeChannel p callback]
  (.add ^CopyOnWriteArrayList (.callbacks p) callback))

(defn canonical-probe-name
  "Returns the canonical representation of a probe name.

   `:foo:bar` => \"foo:bar\"
   `[:foo \"bar:baz\"] => \"foo:bar:baz\""
  [x]
  (cond
    (string? x) x
    (keyword? x) (name x)
    (sequential? x) (->> x (map canonical-probe-name) (interpose ":") (apply str))
    :else (str x)))

(def ^ConcurrentHashMap probes (ConcurrentHashMap.))
(def new-probe-broadcaster (c/channel* :grounded? true, :permanent? true))

(defn on-new-probe [callback]
  (c/receive-all new-probe-broadcaster callback))

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

(def
  ^{:doc
    "Returns a probe channel for the given name.  Keywords will be converted to simple strings, and
     sequences will be joined with ':' characters.

     [:a \"b\" [1 2 3]] => \"a:b:1:2:3\""}
  probe-channel (probe-channel-generator #(probe-channel- % false)))

(def
  ^{:doc
    "Like probe-channel, but if the probe is not active every message enqueued into the channe will be
     logged as an error."}
  error-probe-channel (probe-channel-generator #(probe-channel- % true)))

(defn probe-names
  "Returns a list of all existing probes."
  []
  (keys probes))

(defn select-probes
  "Activates all probes that match the given strings or regexes, and returns a channel that will emit
   all messages from these probes."
  [& wildcard-strings-or-regexes]
  (let [ch (c/channel)
        regexes (map
                  #(if (string? %)
                     (-> % (str/replace "*" ".*") re-pattern)
                     %)
                  wildcard-strings-or-regexes)
        callback (fn [id]
                   (when (some #(re-matches % id) regexes)
                     (c/siphon (probe-channel id) ch)))]
    (c/receive-all new-probe-broadcaster callback)
    (c/on-closed ch #(c/cancel-callback new-probe-broadcaster callback))
    (doseq [id (keys probes)]
      (callback id))
    ch))

;;;

(defn probe-result
  "Allows an async-result to be treated like a probe-channel that only accepts a single value
   before deactivating."
  [result]
  (when-not (r/async-result? result)
    (throw (IllegalArgumentException. "probe-result must be given a result-channel")))
  (reify
    lamina.core.utils.IEnqueue
    (enqueue [_ msg]
      (enqueue result msg))
    lamina.trace.probe.IProbe
    (probe-enabled? [_]
      (not (r/result result)))))

;;;

(deftype+ SympatheticProbeChannel
  [^AtomicBoolean enabled?
   receiver
   emitter]
  lamina.core.utils.IEnqueue
  (enqueue [_ msg]
    (g/propagate (c/receiver-node receiver) msg true))
  IProbe
  (probe-enabled? [_]
    (.get enabled?))
  lamina.core.channel.IChannel
  (receiver-node [_]
    (c/receiver-node receiver))
  (emitter-node [_]
    emitter)
  (split-receiver [_]
    (c/split-receiver emitter))
  (toString [_]
    (str emitter)))

(defn sympathetic-probe-channel
  "A channel that only lets messages through if `channel` has downstream channels."
  [channel]
  (let [receiver-channel (c/channel* :grounded? true)
        receiver (c/receiver-node receiver-channel)
        emitter (g/node* :permanent? true)
        enabled? (AtomicBoolean. false)]

    ;; bridge the upstream and downstream nodes whenever the source channel is active
    (g/on-state-changed (c/emitter-node channel) nil
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
