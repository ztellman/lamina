;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.graph.propagator
  (:use
    [potemkin]
    [lamina.core.graph.core]
    [lamina.core.utils])
  (:require
    [lamina.core.lock :as l]
    [lamina.core.result :as r]
    [lamina.core.graph.node :as n]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.lock
     AsymmetricLock]
    [lamina.core.graph.core
     Edge]
    [java.util.concurrent.atomic
     AtomicBoolean]
    [java.util.concurrent
     ConcurrentHashMap]))

(deftype+ CallbackPropagator [callback]
  IDescribed
  (description [_] (describe-fn callback))
  IError
  (error [_ _ force?])
  IPropagator
  (close [_ force?])
  (transactional [_] false)
  (downstream [_] nil)
  (propagate [_ msg _]
    (try
      (callback msg)
      (catch Exception e
        (log/error e "Error in permanent callback.")))))

(defn callback-propagator [callback]
  (CallbackPropagator. callback))

(deftype+ BridgePropagator [description callback downstream]
  IDescribed
  (description [_] description)
  IError
  (error [_ err force?]
    (doseq [^Edge e downstream]
      (error (.next e) err force?)))
  IPropagator
  (close [_ force?]
    (doseq [^Edge e downstream]
      (close (.next e) force?)))
  (downstream [_] downstream)
  (propagate [_ msg _] (callback msg))
  (transactional [this]
    (doseq [n (downstream-propagators this)]
      (transactional n))))

(deftype+ TerminalPropagator [description]
  IDescribed
  (description [_] description)
  IError
  (error [_ err force?])
  IPropagator
  (close [_ force?])
  (downstream [_] nil)
  (propagate [_ _ _] nil)
  (transactional [_] nil))

(defn terminal-propagator [description]
  (TerminalPropagator. description))

;;;

(defn close-and-clear [lock ^AtomicBoolean closed? ^ConcurrentHashMap downstream]
  (l/with-exclusive-lock lock
    (.set closed? true)
    (let [channels (vals downstream)]
      (.clear ^ConcurrentHashMap downstream)
      channels)))

(deftype+ DistributingPropagator
  [facet
   generator
   ^AsymmetricLock lock
   ^AtomicBoolean closed?
   ^AtomicBoolean transactional?
   ^ConcurrentHashMap downstream]
  IDescribed
  (description [_] "distributor")
  IError
  (error [_ err force?]
    (doseq [n (close-and-clear lock closed? downstream)]
      (error n err force?)))
  IPropagator
  (close [_ force?]
    (doseq [n (close-and-clear lock closed? downstream)]
      (close n force?)))
  (transactional [_]
    (let [downstream
          (l/with-exclusive-lock
            (when (.compareAndSet transactional? false true)
              (vals downstream)))]
      (doseq [n downstream]
        (transactional n))))
  (downstream [_]
    (map #(edge nil %) (vals downstream)))
  (propagate [this msg _]
    (try
      (let [id (facet msg)
            id* (if (nil? id)
                  ::nil
                  id)]
        (if-let [n (l/with-lock lock
                     (when-not (.get closed?)
                       (if-let [n (.get downstream id*)]
                         n
                         (let [n (generator id)]
                           
                           ;; make it transactional, if necessary
                           (when (.get transactional?)
                             (transactional n))
                           
                           ;; check if another channel's already slotted in
                           (or (.putIfAbsent downstream id* n)
                             (do
                               ;; if not, hook into the callbacks
                               (r/subscribe (n/closed-result n)
                                 (r/result-callback
                                   (fn [_] (.remove downstream id*))
                                   (fn [err] (error this err false))))
                               n))))))]
          (propagate n msg true)
          :lamina/closed!))
      (catch Exception e
        (log/error e "error in distributor")
        (error this e false)))))

(defn distributing-propagator [facet generator]
  (DistributingPropagator.
    facet
    generator
    (l/asymmetric-lock)
    (AtomicBoolean. false)
    (AtomicBoolean. false)
    (ConcurrentHashMap.)))

;;;

(defn bridge [src dsts callback
              {:keys [edge-description
                      node-description
                      upstream?
                      downstream?]
               :or {upstream? true
                    downstream? true}}]
  
  (assert (or node-description edge-description))
  
  (let [downstream (to-array (map #(edge nil %) dsts))
        n (BridgePropagator. node-description
            (fn [x]
              (try*
                (callback x)
                (catch Throwable e
                  (log/error e (str "error in " (or node-description edge-description)))
                  (error src e false))))
            downstream)
        upstream (edge edge-description n)]
    
    (n/link src n upstream
      nil
      (fn [_]
        (when downstream?
          (n/on-state-changed src nil (n/downstream-callback src n)))
        (when upstream?
          (doseq [dst dsts]
            (n/on-state-changed dst nil (n/upstream-callback src n true))))))))

