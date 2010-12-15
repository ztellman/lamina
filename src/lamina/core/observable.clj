;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.observable
  (:use
    [clojure.walk])
  (:import
    [java.util.concurrent
     ScheduledThreadPoolExecutor
     TimeUnit]))

;;;

(def empty-value ::empty)

(defprotocol Observer
  (on-message [this msgs])
  (on-close [this])
  (on-observers-changed [this observers]))

(defn observer
  ([message-callback]
     (observer message-callback nil nil))
  ([message-callback close-callback]
     (observer message-callback close-callback nil))
  ([message-callback close-callback observers-callback]
     (reify Observer
       (on-message [_ msgs]
	 (when message-callback
	   (message-callback msgs)))
       (on-close [_]
	 (when close-callback
	   (close-callback)))
       (on-observers-changed [_ observers]
	 (when observers-callback
	   (observers-callback observers))))))

;;;

(defprotocol ObservableProtocol
  (subscribe [this observer-map])
  (unsubscribe [this keys])
  (message [this msgs])
  (close [this])
  (closed? [this]))

(defmacro modify-observers [observers closed? false-case f args]
  `(if ~closed?
     (do
       ~false-case
       nil)
     (let [observers# (apply swap! ~observers ~f ~args)]
       (doseq [o# (vals observers#)]
	 (on-observers-changed o# observers#))
       observers#)))

(deftype Observable [observers closed?]
  ObservableProtocol
  (subscribe [_ m]
    (modify-observers observers @closed?
      false
      merge m))
  (unsubscribe [_ ks]
    (modify-observers observers @closed?
      false
      dissoc ks))
  (message [_ msgs]
    (when-not (empty? msgs)
      (if @closed?
	false
	(let [s (vals @observers)]
	  (if (= 1 (count s))
	    (not= ::false (on-message (first s) msgs))
	    (do
	      (doseq [o s]
		(on-message o msgs))
	      true))))))
  (close [_]
    (if-not (compare-and-set! closed? false true)
      false
      (do
	(doseq [o (vals @observers)]
	  (on-close o))
	true)))
  (closed? [_]
    @closed?))

(defn observable []
  (Observable. (atom {}) (atom false)))

(defn observable? [o]
  (instance? Observable o))

(deftype ConstantObservable [observers val]
  ObservableProtocol
  (subscribe [_ m]
    (modify-observers observers (not= ::empty @val)
      (do
	(doseq [o (vals m)]
	  (on-message o [@val]))
	true) 
      merge m))
  (unsubscribe [_ ks]
    (modify-observers observers (not= ::empty @val)
      false
      dissoc ks))
  (message [_ msgs]
    (when-not (empty? msgs)
      (let [msg (first msgs)]
	(when (compare-and-set! val ::empty msg)
	  (let [s (vals @observers)]
	    (reset! observers nil)
	    (doseq [o s]
	      (on-message o [msg]))))))
    false)
  (close [_]
    (throw (Exception. "Constant observables cannot be closed.")))
  (closed? [_]
    false)
  (toString [_]
    (if-not (= ::empty @val)
      (apply str (drop-last (prn-str @val)))
      "")))

(defn constant-observable
  ([]
     (constant-observable ::empty))
  ([message]
     (ConstantObservable. (atom {}) (atom message))))

(defn constant-observable? [o]
  (instance? ConstantObservable o))

(def nil-observable
  (reify ObservableProtocol
    (subscribe [_ _] false)
    (unsubscribe [_ _] false)
    (message [_ _] false)
    (close [_] false)
    (closed? [_] true)))

;;;

(defn siphon [src destination-function-map]
  (do
    (subscribe src
      (zipmap
	(keys destination-function-map)
	(map
	  (fn [[dst f]]
	    (observer
	      (fn [msgs] (message dst (f msgs)))))
	  destination-function-map)))
    (doseq [dst (keys destination-function-map)]
      (subscribe dst
	{src (observer
	       nil
	       (fn []
		 (when (>= 1 (count (unsubscribe src [dst])))
		   (close src))
		 (unsubscribe dst [src]))
	       nil)}))))





