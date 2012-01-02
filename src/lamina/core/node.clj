;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.node
  (:use
    [useful.datatypes :only (make-record assoc-record)])
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
  (downstream-nodes [_]
    "Returns a list of nodes which are downstream of this node.")
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed."))

(defprotocol NodeProtocol
  (predicate-receive [_ predicate false-value result-channel]
    "Straight call to the queue which cannot be cancelled. Intended for (read-channel ...).")
  (receive [_ name callback]
    "Cancellable call to the queue. Intended for (receive ...).")
  (close [_]
    "Closes the node. This is a permanent state change.")
  (error [_ error]
    "Puts the node into an error mode. This is a permanent state change.")
  (link [_ name node]
    "Adds a downstream node. The link should be cancelled using (cancel this name).")
  (unlink [_ node]
    "Removes a downstream node. Returns true if there was any such downstream node, false
     otherwise.")
  (on-state-changed [_ name callback]
    "Adds a callback which takes two paramters: [state error].  Possible states are
     ::zero, ::one, ::many, ::closed, ::drained, and ::error.")
  (cancel [_ name]
    "Cancels a callback registered by link, on-state-changed, or receive.  Returns true
     if a callback with that identifier is currently registered.")
  (closed? [_])
  (drained? [_]))

(defrecord NodeState
  [mode
   queue
   consumed?
   transactional?
   permanent?
   error
   next])

(defmacro enqueue-and-release [lock state msg persist?]
  `(if-let [q# (.queue ~state)]
     (q/enqueue q# ~msg true #(l/release ~lock))
     (l/release ~lock)))

(defmacro set-state! [state state-val & key-vals]
  `(let [val# (assoc-record ~state-val ~@key-vals)]
     (set! ~state val#)
     val#))

(defmacro transform-message [node msg transform?]
  `(let [msg# ~msg
         node# ~node
         operator# (.operator node#)
         transform?# ~transform?]
     (if-not (and transform?# operator#)
       msg#
       (try
         (operator# msg#)
         (catch Exception e#
           (error node# e#)
           ::error)))))

(defmacro when-not-drained [[state state-callbacks cancellations] result & body]
  `(let [result# ~result]
     (if (= :lamina/drained! (r/error-value result# nil))
       (do
         ;; we can do this outside of a lock because this is the only remaining
         ;; possible state transition; it doesn't matter if we're competing with
         ;; another thread
         (set-state! ~state ~state
           :mode ::drained
           :queue (q/drained-queue))
         (doseq [l# ~state-callbacks]
           (l# ::drained nil))
         (.clear ~state-callbacks)
         (.clear ~cancellations)
         result#)
       (do
         ~@body))))

(defmacro ensure-queue [state]
  `(let [^NodeState s# ~state]
     (if (.queue s#)
       s#
       (set-state! ~state s#
         :queue (q/queue)
         :consumed? true))))

(defmacro close-node! [state s]
  `(let [s# ~s
         q# (q/closed-copy (.queue s#))
         drained?# (q/drained? q#)]
     (set-state! ~state s#
       :mode (if drained?# ::drained ::closed)
       :queue (if drained?# (q/drained-queue) q#)
       :next nil)))

(deftype Node
  [^AsymmetricLock lock
   operator
   ^:volatile-mutable ^NodeState state
   ^ConcurrentHashMap cancellations
   ^CopyOnWriteArrayList state-callbacks]

  l/AsymmetricLockProtocol

  (acquire [_] (l/acquire lock))
  (acquire-exclusive [_] (l/acquire-exclusive lock))
  (release [_] (l/release lock))
  (release-exclusive [_] (l/release-exclusive lock))
  (try-acquire [_] (l/try-acquire lock))
  (try-acquire-exclusive [_] (l/try-acquire-exclusive lock))

  PropagateProtocol

  (downstream-nodes [_]
    (let [state state]
      (case (.mode state)
        ::one [(.next state)]
        ::many (seq (.next state))
        nil)))

  (propagate [this msg transform?]
    (let [msg (transform-message this msg transform?)]
      (case msg

        ::error
        :lamina/error!
        
        ::false
        :lamina/filtered
        
        (do
          ;; acquire the lock before we look at the state
          (l/acquire lock)
          (let [state state]
            (case (.mode state)

              ::closed
              :lamina/closed!

              ::error
              :lamina/error!
              
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

                    (let [^Node node node
                          msg (transform-message node msg true)]

                      (case msg

                        ::error
                        :lamina/error!

                        ::false
                        :lamina/filtered
                        
                        (do
                          (l/acquire (.lock node))
                          (let [^NodeState state (.state node)]
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
                  (propagate node msg true))

                :lamina/split)))))))

  NodeProtocol

  (predicate-receive [_ predicate false-value result-channel]
    (let [s (l/with-exclusive-lock*
              (ensure-queue state))
          result (q/receive (.queue s) predicate false-value result-channel)]
      (when-not-drained [state state-callbacks cancellations] result
        result)))

  (receive [_ name callback]
    (let [x (l/with-exclusive-lock*
              (if-let [v (.get cancellations name)]
                
                ;; there's already a pending receive op, just return the result-channel
                (if (r/result-channel? v)
                  v
                  ::invalid-name)

                ;; return the current state
                (ensure-queue state)))]
      (cond
        (= ::invalid-name x)
        (throw (IllegalStateException. "Invalid callback identifier used for (receive ...)"))

        (r/result-channel? x)
        x

        :else
        (let [s x
              result (q/receive (.q s) nil nil nil)]
          (when-not-drained [state state-callbacks cancellations] result
            (.put cancellations result #(q/cancel-receive (.q s) result))
            (let [f (fn [_] (.remove cancellations name))]
              (r/subscribe result (r/result-callback f f)))
            result)))))

  (close [_]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (let [s state]
                   (case (.mode s)
                     
                     (::zero ::one ::many)
                     (close-node! state s)
                     
                     nil)))]
        ;; signal state change
        (do
          (doseq [l state-callbacks]
            (l (.mode s) nil))
          (.clear state-callbacks)
          true)

        ;; state has already been permanently changed
        false)))

  (error [_ err]
    (io! "Cannot modify node while in transaction."
      (if-let [old-queue (l/with-exclusive-lock* lock
                           (let [s state]
                             (case (.mode s)
                               
                               (::zero ::one ::many ::closed)
                               (let [q (.queue s)]
                                 (set-state! state s
                                   :mode ::error
                                   :queue (q/error-queue err)
                                   :error err
                                   :next nil)
                                 q)
                               
                               nil)))]
        ;; signal state change
        (do
          (error old-queue err)
          (doseq [l state-callbacks]
            (l ::error err))
          (.clear state-callbacks)
          (.clear cancellations)
          true)
        
        ;; state has already been permanently changed
        false)))

  (link [this name node]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (when-not (.containsKey cancellations name)
                   (let [s state
                         new-state
                         (case (.mode s)

                           ::zero
                           (do
                             (set-state! state s
                               :mode ::one
                               :queue (when (.consumed? s) (q/queue))
                               :next node)
                             (assoc-record s
                               :mode ::one
                               :next node))
                         
                           ::one
                           (set-state! state s
                             :mode ::many
                             :next (CopyOnWriteArrayList. ^objects (to-array [(.next s) node])))
                         
                           ::many
                           (do
                             (.add ^CopyOnWriteArrayList (.next s) node)
                             s)

                           ::closed
                           (do
                             (set-state! state s
                               :mode ::drained
                               :queue (q/drained-queue))
                             ;; make sure we return the original queue
                             (assoc-record s
                               :mode ::drained))
                         
                           nil)]

                     (when new-state
                       (.put cancellations name #(unlink this node))
                       new-state))))]

        (do
          ;; if we've gone from ::zero -> ::one or ::closed -> ::drained,
          ;; send all queued messages
          (when-let [q (and
                         (case (.mode s)
                           (::one ::drained) true
                           false)
                         (.queue s))]
            (doseq [msg (q/ground q)]
              (propagate (.next s) msg true)))

          ;; notify all state-changed listeners
          (doseq [l state-callbacks]
            (l (.mode ^NodeState s) nil))

          true)

        ;; we're already in ::closed or ::error mode
        false)))
  
  (unlink [_ node]
    (io! "Cannot modify node while in transaction."
      (if-let [s (l/with-exclusive-lock* lock
                   (let [s state]
                     (case s
                       ::zero
                       nil
                       
                       ::one
                       (when (= node (.next s))
                         (if-not (.permanent? s)
                           (close-node! state s)
                           (set-state! state s
                             :mode ::zero
                             :queue (when (.consumed? s) (q/queue)))))
                       
                       ::many
                       (let [l ^CopyOnWriteArrayList (.next s)]
                         (when (.remove l node)
                           (if (= 1 (.size l))
                             (set-state! state s
                               :mode ::one
                               :next (.get l 0))
                             ::state-unchanged)))
                           
                           nil)))]
        (do
          (when-not (= s ::state-unchanged)
            (doseq [l state-callbacks]
              (l (.mode ^NodeState s) nil)))
          true)
        false)))

  (on-state-changed [_ name callback]
    (let [s (l/with-exclusive-lock lock
              (when (or (nil? name) (not (.containsKey cancellations name)))
                (let [s state]
                  (case (.mode s)
                    
                    (::zero ::one ::many)
                    (do
                      (.add state-callbacks callback)
                      (when name
                        (.put cancellations name #(.remove state-callbacks callback))))
                    
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
      false))

  (closed? [_]
    (case (.mode state)
      (::closed ::drained) true
      false))

  (drained? [_]
    (case (.mode state)
      ::drained true
      false)))

;;;

(deftype CallbackNode [callback]
  PropagateProtocol
  (downstream-nodes [_]
    nil)
  (propagate [_ msg _]
    (callback msg)))

(defn callback-node [callback]
  (CallbackNode. callback))

;;;

(defn predicate-operator [predicate]
  #(if (predicate %)
     %
     ::false))

;;;

(defn siphon-callback [src dst]
  (fn [state _]
    (case state
      (::closed :error) (cancel src dst)
      nil)))

(defn join-callback [dst]
  (fn [state err]
    (case state
      ::closed (close dst)
      ::error (error dst err)
      nil)))

(defn siphon [src dst]
  (let [success? (link src dst dst)]
    (when success?
      (on-state-changed dst nil (siphon-callback src dst)))
    success?))

(defn join [src dst]
  (let [success? (siphon src dst)]
    (when success?
      (on-state-changed src nil (join-callback dst)))
    success?))

;;;

(defn node [operator]
  (when-not (fn? operator)
    (throw (Exception. (str (pr-str operator) " is not a valid function."))))
  (Node.
    (l/asymmetric-lock)
    operator
    (make-record NodeState
      :mode ::zero
      :queue (q/queue))
    (ConcurrentHashMap.)
    (CopyOnWriteArrayList.)))

(defn upstream-node [operator downstream-node]
  (let [n (Node.
            (l/asymmetric-lock)
            operator
            (make-record NodeState
              :mode ::one
              :next downstream-node)
            (ConcurrentHashMap.)
            (CopyOnWriteArrayList. (to-array [(join-callback downstream-node)])))]
    (on-state-changed downstream-node nil (siphon-callback n downstream-node))
    n))

(defn node? [x]
  (instance? Node x))

;;;

(defn walk-nodes [f exclusive? n]
  (let [s (downstream-nodes n)]
    (->> s (filter node?) (l/acquire-all exclusive?))
    (when (node? n)
      (f n)
      (if exclusive?
        (l/release-exclusive n)
        (l/release n)))
    (doseq [n s]
      (walk-nodes f exclusive? n))))

(defn node-seq [n]
  (tree-seq
    (comp seq downstream-nodes)
    downstream-nodes
    n))

;;;

(defn on-closed [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state _]
        (case state
          (::closed ::drained)
          (when (compare-and-set! latch false true)
            (callback))

          nil)))))

(defn on-drained [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state _]
        (case state
          ::drained
          (when (compare-and-set! latch false true)
            (callback))

          nil)))))
