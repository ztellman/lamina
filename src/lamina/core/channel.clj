;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.channel
  (:require
    [lamina.core.node :as n]
    [lamina.core.queue :as q])
  (:import
    [lamina.core.node
     Node]
    [java.io
     Writer]))

(set! *warn-on-reflection* true)

;;;

(defrecord Channel [^Node receiver ^Node emitter]
  Object
  (toString [_]
    (if-let [q (n/queue emitter)]
      (str "<== " (-> q q/messages vec str))
      (str "<== []"))))

(defn channel
  [& messages]
  (let [emitter (n/node identity (seq messages))
        receiver (n/upstream-node identity emitter)]
    (Channel. receiver emitter)))

;;;

(defn enqueue [^Channel channel message]
  (n/propagate (.receiver channel) message true))

(defn receive [^Channel channel callback]
  (n/receive (.emitter channel) callback callback))

(defn read-channel [^Channel channel predicate false-value result-channel]
  (n/predicate-receive (.emitter channel) predicate false-value result-channel))

(defn receive-all [^Channel channel callback]
  (n/link (.emitter channel) callback (n/callback-node callback) nil))

(defn fork [^Channel channel]
  (let [emitter (n/node identity)
        receiver (n/upstream-node emitter)]
    (n/link (.receiver channel) receiver receiver
      #(when-let [q (n/queue (.emitter channel))]
         (-> emitter n/queue (q/append (q/messages q)))))
    (Channel. receiver emitter)))

(defn splice [^Channel emitter ^Channel receiver]
  (Channel. (.receiver receiver) (.emitter emitter))) 

(defn close [^Channel channel]
  (n/close (.receiver channel)))

(defn error [^Channel channel err]
  (n/error (.receiver channel) err))

(defn closed? [^Channel channel]
  (n/closed? (.receiver channel)))

(defn drained? [^Channel channel]
  (n/drained? (.emitter channel)))

(defn on-closed [^Channel channel callback]
  (n/on-closed (.receiver channel) callback))

(defn on-drained [^Channel channel callback]
  (n/on-drained (.emitter channel) callback))

;;;

(defmethod print-method Channel [o ^Writer w]
  (.write w (str o)))
