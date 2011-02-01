;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.observable
  (:use
    [clojure.walk])
  (:import
    [java.util.concurrent
     ScheduledThreadPoolExecutor
     TimeUnit
     Semaphore]))

;;;

(def empty-value ::empty)

(defprotocol ObserverProtocol
  (on-message [this msgs])
  (on-close [this])
  (on-observers-changed [this observers]))

(defn observer
  ([message-callback]
     (observer message-callback nil nil))
  ([message-callback close-callback]
     (observer message-callback close-callback nil))
  ([message-callback close-callback observers-callback]
     (reify ObserverProtocol
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
  `(if-let [observers# (when-not ~closed?
			 (apply swap! ~observers ~f ~args))]
     (do
       (doseq [o# (vals observers#)]
	 (on-observers-changed o# observers#))
       observers#)
     (do
       ~false-case
       nil)))

(defmacro with-observable [observable & body]
  `(try
     (.acquire ^Semaphore (.semaphore ~observable))
     ~@body
     (finally
       (.release ^Semaphore (.semaphore ~observable)))))

(defrecord Observable [observers closed? ^Semaphore semaphore]
  ObservableProtocol
  (subscribe [_ m]
    (modify-observers observers @closed?
      (do
	(doseq [o (vals m)]
	  (on-close o)))
      merge m))
  (unsubscribe [_ ks]
    (modify-observers observers false
      false
      dissoc ks))
  (message [this msgs]
    (when-not (empty? msgs)
      (if @closed?
	false
	(with-observable this
	  (let [s (vals @observers)]
	    (if (= 1 (count s))
	      (not= ::false (on-message (first s) msgs))
	      (do
		(doseq [o s]
		  (on-message o msgs))
		true)))))))
  (close [this]
    (if-not (compare-and-set! closed? false true)
      false
      (do
	(doseq [o (vals @observers)]
	  (on-close o))
	(unsubscribe this (keys @observers))
	true)))
  (closed? [_]
    @closed?))

(defmacro lock-observable [^Observable observable & body]
  `(try
     (.acquire ^Semaphore (:semaphore ~observable) Integer/MAX_VALUE)
     ~@body
     (finally
       (.release ^Semaphore (:semaphore ~observable) Integer/MAX_VALUE))))

(defn observable []
  (Observable. (atom {}) (atom false) (Semaphore. Integer/MAX_VALUE)))

(defn permanent-observable []
  (with-meta (observable) {::permanent true}))

(defn observable? [o]
  (instance? Observable o))

(defmacro safe-modify-observers [observers closed? false-case f args]
  `(if-let [observers# (locking ~observers
			 (when-not ~closed?
			   (apply swap! ~observers ~f ~args)))]
     (doseq [o# (vals observers#)]
       (on-observers-changed o# observers#))
     ~false-case))

(defrecord ConstantObservable [observers val]
  ObservableProtocol
  (subscribe [_ m]
    (safe-modify-observers observers (not= ::empty @val)
      (do
	(doseq [o (vals m)]
	  (on-message o [@val]))
	true) 
      merge m))
  (unsubscribe [_ ks]
    (safe-modify-observers observers (not= ::empty @val)
      false
      dissoc ks))
  (message [_ msgs]
    (when-not (empty? msgs)
      (let [msg (first msgs)]
	(when (compare-and-set! val ::empty msg)
	  (locking observers
	    (let [s (vals @observers)]
	      (reset! observers nil)
	      (doseq [o s]
		(on-message o [msg])))))))
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

(defn siphon
  ([src destination-function-map]
     (siphon src destination-function-map 0 false))
  ([src destination-function-map observer-threshold propagate-close?]
     (do
       (subscribe src
	 (zipmap
	   (keys destination-function-map)
	   (map
	     (fn [[dst f]]
	       (observer
		 (fn [msgs] (message dst (f msgs)))
		 (when propagate-close?
		   (fn [] (close dst)))
		 nil))
	     destination-function-map)))
       (doseq [dst (keys destination-function-map)]
	 (subscribe dst
	   {src (observer
		  nil
		  (fn []
		    (when (and
			    (not (::permanent (meta src)))
			    (<= (count (unsubscribe src [dst])) observer-threshold))
		      (close src))
		    (unsubscribe dst [src]))
		  nil)})))))





