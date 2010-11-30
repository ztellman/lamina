;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.observable
  (:import
    [java.util.concurrent
     ScheduledThreadPoolExecutor
     TimeUnit]))

;;;

(defprotocol Observer
  (on-message [this msgs])
  (on-close [this]))

(defn observer
  ([message-callback]
     (observer message-callback nil))
  ([message-callback close-callback]
     (reify Observer
       (on-message [_ msgs]
	 (when message-callback
	   (message-callback msgs)))
       (on-close [_]
	 (when close-callback
	   (close-callback))))))

;;;

(defprotocol Observable
  (subscribe [this observer-map])
  (unsubscribe [this keys])
  (message [this msgs])
  (observers [this])
  (close [this])
  (closed? [this]))

(defn- close-observable [closed observers]
  (dosync
    (ref-set closed true)
    (let [o (vals @observers)]
      (ref-set observers nil)
      o)))

(defmacro when-not-closed [closed & body]
  `(if (deref ~closed)
     false
     (do
       ~@body
       true)))

(defn observable []
  (let [closed (ref false)
	observers (ref {})]
    (reify Observable
      (subscribe [_ m]
	(when-not-closed closed
	  (dosync (alter observers merge m))))
      (unsubscribe [_ ks]
	(when-not-closed closed
	  (dosync (apply alter observers dissoc ks))))
      (observers [_]
	@observers)
      (message [_ msgs]
	(when-not-closed closed
	  (let [observers @observers]
	    (if (= 1 (count observers))
	      (not= ::false (on-message (first (vals observers)) msgs))
	      (doseq [o (vals observers)]
		(on-message o msgs))))))
      (close [_]
	(when-not-closed closed
	  (doseq [o (close-observable closed observers)]
	    (on-close o))))
      (closed? [_]
	@closed))))

(def closed-observable
  (reify Observable
    (subscribe [_ _] false)
    (unsubscribe [_ _] false)
    (observers [_] nil)
    (message [_ _] false)
    (close [_] false)
    (closed? [_] true)))

;;;

(defn siphon
  ([src dst]
     (siphon identity src dst))
  ([f src dst]
     (subscribe src
       {dst (observer
	      #(when-not (message dst (map f %))
		 (unsubscribe src [dst])
		 ::false))})))

(defn siphon-when [pred src dst]
  (subscribe src
    {dst (observer
	   #(when-not (message dst (filter pred %))
	      (unsubscribe src [dst])
	      ::false))}))

(defn- take-while* [pred s]
  (loop [src s, dst []]
    (if (empty? src)
      [true dst]
      (if (pred (first src))
	(recur (rest src) (conj dst (first src)))
	[false dst]))))

(defn siphon-while [pred src dst]
  (let [done? (ref false)]
    (subscribe src
      {dst (observer
	     #(let [msgs (dosync
			   (ensure done?)
			   (when-not @done?
			     (let [[complete msgs] (take-while* pred %)]
			       (if-not complete
				 (do (ref-set done? true) msgs)
				 msgs))))]
		(when (or (not (message dst msgs)) @done?)
		  (unsubscribe src [dst])
		  ::false)))})))



