;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.node
  (:require
    [lamina.core.result :as r]
    [lamina.core.queue :as q]
    [lamina.core.lock :as l])
  (:import
    [lamina.core.lock AsymmetricLock]
    [java.util
     Collection]
    [java.util.concurrent
     CopyOnWriteArrayList
     ConcurrentHashMap]))

;;;

(defprotocol PropagateProtocol
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed."))

(defprotocol NodeProtocol
  (predicate-receive [_ predicate false-value result-channel]
    "Straight call to the queue which cannot be cancelled. Intended for (read-channel ...).")
  (receive [_ name callback]
    "Cancellable call to the queue. Intended for (receive ...).")
  (close [_])
  (error [_ error])
  (link [_ name node])
  (unlink [_ node])
  (on-state-changed [_ name callback])
  (cancel [_ name]))

(deftype NodeState
  [mode
   queue
   ^boolean consumed?
   ^boolean transactional?
   error
   next])

(defmacro enqueue-and-release [lock state msg persist?]
  `(if-let [q# (.queue ~state)]
     (q/enqueue q# ~msg true #(l/release ~lock))
     (l/release ~lock)))

(defmacro get-or-create-queue [state]
  `(let [^NodeState s# ~state]
     (if-let [q# (.queue s#)]
       q#
       (let [q# (q/queue)]
         (set! ~state (NodeState.
                        (.mode s#)
                        q#
                        true
                        (.transactional? s#)
                        (.error s#)
                        (.next s#)))
         q#))))

(deftype Node
  [^AsymmetricLock lock
   operator
   ^:volatile-mutable ^NodeState state
   ^ConcurrentHashMap cancellations
   ^CopyOnWriteArrayList state-callbacks]

  PropagateProtocol

  (propagate [this msg transform?]
    (let [msg (try
                (if (and transform? operator)
                  (operator msg)
                  msg)
                (catch Exception e
                  (error this e)
                  ::error))]
      (case msg

        (::error ::false)
        false
        
        (do
          ;; acquire the lock before we look at the state
          (l/acquire lock)
          (let [state state]
            (case (.mode state)
              
              ::zero
              (enqueue-and-release lock state msg true)
              
              ::one
              (do
                (enqueue-and-release lock state msg false)

                ;; walk the chain of nodes until there's a split
                (loop [node (.next state), msg msg]
                  
                  (if-not (instance? Node node)

                    ;; if it's not a normal node, forward the message
                    (propagate node msg true)

                    (let [node ^Node node
                          operator (.operator node)
                          msg (try
                                (if operator
                                  (operator msg)
                                  msg)
                                (catch Exception e
                                  (error node e)
                                  ::error))]

                      (case msg

                        (::error ::false)
                        false
                        
                        (do
                          (l/acquire (.lock node))
                          (let [state ^NodeState (.state node)]
                            (if-not (= ::one (.mode state))
                              
                              ;; if there's not just one downstream node, forward the message
                              (do
                                (l/release (.lock node))
                                (propagate node msg false))

                              ;; otherwise, rinse and repeat
                              (do
                                (enqueue-and-release (.lock node) state msg false)
                                (recur (.next state) msg))))))))))
              
              ::many
              (do
                (enqueue-and-release lock state msg false)

                ;; send message to all nodes
                (doseq [node (.next state)]
                  (propagate node msg true)))

              ;; otherwise, just stop
              false))))))

  NodeProtocol

  (predicate-receive [_ predicate false-value result-channel]
    (let [proxy-result (when result-channel (r/result-channel))
          result (l/with-exclusive-lock*
                   (q/receive (get-or-create-queue state) predicate false-value proxy-result))]
      (when proxy-result
        (r/siphon-result proxy-result result-channel))
      result))

  (receive [_ name callback]
    (l/with-exclusive-lock*
      (if-let [v (.get cancellations name)]

        ;; there's already a pending receive op, just return the result-channel
        (if (r/result-channel? v)
          v
          (throw (IllegalStateException. "Non-receive callback used for receive operation.")))

        ;; receive from queue and set up cancellation callbacks
        (let [result-channel (q/receive (get-or-create-queue state) nil nil nil)]
                       
          ;; set up cancellation
          (.put cancellations name result-channel)
                       
          (let [f (fn [_] (.remove cancellations name))]
            (r/subscribe result-channel (r/result-callback f f)))
                       
          result-channel))))

  (close [_]
    (io! "Cannot modify node while in transaction."
      (if (l/with-exclusive-lock* lock
            (let [s state]
              (case (.mode s)
                
                (::zero ::one ::many)
                (do
                  (set! state (NodeState. ::closed
                                (when (.queue s) (q/closed-copy (.queue s)))
                                (.consumed? s)
                                (.transactional? s)
                                nil
                                nil))
                  true)
                
                false)))
        ;; signal state change
        (do
          (doseq [l state-callbacks]
            (l ::closed nil))
          true)

        ;; state has already been permanently changed
        false)))

  (error [_ err]
    (io! "Cannot modify node while in transaction."
      (if-let [old-queue (l/with-exclusive-lock* lock
                           (let [s state]
                             (case (.mode s)
                               
                               (::zero ::one ::many)
                               (let [q (.queue s)]
                                 (set! state (NodeState. ::error
                                               (q/error-queue err)
                                               (.consumed? s)
                                               (.transactional? s)
                                               err
                                               nil))
                                 q)
                               
                               nil)))]
        ;; signal state change
        (do
          (error old-queue err)
          (doseq [l state-callbacks]
            (l ::error err))
          true)
        
        ;; state has already been permanently changed
        false)))

  (link [this name node]
    (io! "Cannot modify node while in transaction."
      (if-let [s
               (l/with-exclusive-lock* lock
                 (when-not (.containsKey cancellations name)
                   (let [s state
                       
                        new-state
                        (case s
                          ::zero
                          (NodeState. ::one
                            (when (.consumed? s) (.queue s))
                            (.consumed? s)
                            (.transactional? s)
                            nil
                            node)
                         
                          ::one
                          (NodeState. ::many
                            (.queue s)
                            (.consumed? s)
                            (.transactional? s)
                            nil
                            (CopyOnWriteArrayList. ^objects (to-array [(.next s) node])))
                         
                          ::many
                          (do
                            (.add ^CopyOnWriteArrayList (.next s) node)
                            s)
                         
                          nil)]
                    ;; set up cancellation
                    (when new-state
                      (.put cancellations name
                        #(unlink this node)))
                   
                    ;; only peg this as a state change if they're not identical
                    (when-not (identical? s new-state)
                      (set! state new-state)
                      new-state))))]
        (let [s ^NodeState s]
          ;; if this is our first downstream link, send all queued messages
          (when (= ::one (.mode s))
            (doseq [msg (q/ground (.queue s))]
              (propagate (.next s) msg true)))

          ;; notify all state-changed listeners
          (doseq [l state-callbacks]
            (l (.mode ^NodeState s) nil))

          true)

        false)))
  
  (unlink [_ node]
    (io! "Cannot modify node while in transaction."
      (if-let [s (l/with-exclusive-lock* lock
                   (let [s state

                         new-state
                         (case s
                           ::zero
                           nil
                           
                           ::one
                           (when (= node (.next s))
                             (NodeState. ::zero
                               (or (.queue s) (q/queue))
                               (.consumed? s)
                               (.transactional? s)
                               nil
                               nil))
                           
                           ::many
                           (let [l ^CopyOnWriteArrayList (.next s)]
                             (when (.remove l node)
                               (if (= 1 (.size l))
                                 (NodeState. ::one
                                   (.queue s)
                                   (.consumed? s)
                                   (.transactional? s)
                                   nil
                                   (.get l 0))
                                 s)))
                           
                           nil)]
                     (cond
                       (= nil new-state)
                       false

                       (identical? new-state s)
                       ::state-unchanged

                       :else
                       (do
                         (set! state new-state)
                         new-state))))]
        (do
          (when-not (= s ::state-unchanged)
            (doseq [l state-callbacks]
              (l (.mode ^NodeState s) nil)))
          true)
        false)))

  (on-state-changed [_ name callback]
    (let [s (l/with-exclusive-lock lock
              (when-not (.containsKey cancellations name)
                (let [s state]
                  (case (.mode s)
                    
                    (::zero ::one ::many)
                    (do
                      (.add state-callbacks callback)
                      (.put cancellations name #(.remove state-callbacks callback)))
                    
                    nil)
                  s)))]
      (if s
        (do
          (callback (.mode ^NodeState s) (.error ^NodeState s))
          true)
        false)))
  
  (cancel [_ name]
    (if-let [x (l/with-lock lock
                 (.remove cancellations name))]
      (if (r/result-channel? x)
        (l/with-lock lock
          (q/cancel-receive (.queue state) x))
        (do
          (x)
          true))
      false)))

(deftype CallbackNode [callback]
  PropagateProtocol
  (propagate [_ msg _]
    (callback msg)))

;;;

(defn node [operator]
  (Node.
    (l/asymmetric-lock)
    operator
    (NodeState. ::zero (q/queue) false false nil nil)
    (ConcurrentHashMap.)
    (CopyOnWriteArrayList.)))

(defn upstream-node [operator downstream-node]
  (Node.
    (l/asymmetric-lock)
    operator
    (NodeState. ::one nil false false nil downstream-node)
    (ConcurrentHashMap.)
    (CopyOnWriteArrayList.)))

(defn callback-node [callback]
  (CallbackNode. callback))
