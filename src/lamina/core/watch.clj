;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.watch
  (:use
    [lamina.core
     channel
     [utils :only (enqueue)]]))

(defn watch-channel
  "Transforms a watchable `reference` into a channel of values."
  [reference]
  (let [ch (channel @reference)
        callback (fn [_ _ _ val] (enqueue ch val))]
    (add-watch reference callback callback)
    (on-closed ch #(remove-watch reference callback))
    ch))

(defn atom-sink
  "Transforms a `channel` into an atom which updated with the value of each new message."
  ([channel]
     (atom-sink nil channel))
  ([initial-value channel]
     (let [a (atom initial-value)]
       (receive-all channel #(reset! a %))
       a)))
