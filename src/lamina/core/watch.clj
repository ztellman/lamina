;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.watch
  (:use
    [lamina.core channel utils]))

(defn watch-channel
  "something goes here"
  [reference]
  (let [ch (channel)
        callback #(enqueue ch %)]
    (add-watch reference callback callback)
    (on-closed ch #(remove-watch reference callback))
    ch))

(defn atom-sink
  "something goes here"
  ([ch]
     (atom-sink nil ch))
  ([initial-value ch]
     (let [a (atom initial-value)]
       (receive-all ch #(reset! a %))
       a)))
