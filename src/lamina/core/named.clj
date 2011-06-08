;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.named
  (:use
    [lamina.core.channel]))

(def named-channels (ref {}))

(defn named-channel
  "Returns a unique channel for the key.  If no such channel exists,
   a channel is created, and 'creation-callback' is invoked."
  ([key]
     (named-channel key nil))
  ([key creation-callback]
     (let [[created? ch] (dosync
			   (if-let [ch (@named-channels key)]
			     [false ch]
			     (let [ch (permanent-channel)]
			       (commute named-channels assoc key ch)
			       [true ch])))]
       (when (and created? creation-callback)
	 (creation-callback ch))
       ch)))

(defn release-named-channel
  "Forgets the channel associated with the key, if one exists."
  [key]
  (when-let [ch (dosync
		  (let [ch ((ensure named-channels) key)]
		    (alter named-channels dissoc key)
		    ch))]
    (close ch)))
