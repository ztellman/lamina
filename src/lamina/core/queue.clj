;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.queue)

(defprotocol Queue
  (enqueue [this msgs])
  (try-pop [this])
  (receive [this callbacks])
  (listen [this callbacks]))

(defn- take-receiver-messages [receiver msgs]
  (loop [r receiver, s msgs]
    (let [])))

(defn queue []
  (let []
    (reify
      (enqueue [_ msgs]
	)
      (receive [_ callbacks]
	)
      (listen [_ callbacks]
	))))
