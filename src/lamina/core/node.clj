;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.node
  (:use
    [useful.datatypes :only (make-record assoc-record)]
    [lamina.core.threads :only (enqueue-cleanup)])
  (:require
    [lamina.core.result :as r]
    [lamina.core.queue :as q]
    [lamina.core.lock :as l])
  (:import
    [lamina.core.lock
     AsymmetricLock]
    [lamina.core.result
     SuccessResult
     ResultChannel]
    [java.util
     Collection
     Collections
     HashMap
     Map]
    [java.util.concurrent
     CopyOnWriteArrayList
     ConcurrentHashMap]))

(set! *warn-on-reflection* true)

;;;

(deftype NodeState
  [mode ;; ::zero, ::one, ::many, ::split-one, ::split-many, ::closed, ::error
   queue
   consumed?
   transactional?
   permanent?
   error
   next
   split-next])

(defprotocol PropagateProtocol
  (downstream-nodes [_]
    "Returns a list of nodes which are downstream of this node.")
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed."))

(defprotocol NodeProtocol
  (read-node [_] [_ predicate false-value result-channel]
    "Straight call to the queue which cannot be cancelled. Intended for (read-channel ...).")
  (receive [_ name callback]
    "Cancellable call to the queue. Intended for (receive ...).")
  (close [_]
    "Closes the node. This is a permanent state change.")
  (error [_ error]
    "Puts the node into an error mode. This is a permanent state change.")
  (link [_ name node execute-within-lock]
    "Adds a downstream node. The link should be cancelled using (cancel this name).")
  (unlink [_ node]
    "Removes a downstream node. Returns true if there was any such downstream node, false
     otherwise.")
  (^NodeState state [_]
    "Returns the NodeState for this node.")
  (on-state-changed [_ name callback]
    "Adds a callback which takes two paramters: [state error].  Possible states are
     ::zero, ::one, ::many, ::closed, ::drained, and ::error.")
  (cancel [_ name]
    "Cancels a callback registered by link, on-state-changed, or receive.  Returns true
     if a callback with that identifier is currently registered.")
  (ground [_]
    "Consumes and returns all messages currently in the queue.")
  (split [_]))

(declare join-callback siphon-callback)

;;;

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

(defmacro check-for-drained [state state-callbacks cancellations]
  `(let [^NodeState state# ~state]
     (when-let [q# (.queue state#)]
       (when (q/drained? q#)
         (set-state! ~state state#
           :mode ::drained
           :queue (q/drained-queue))
         (doseq [l# ~state-callbacks]
           (l# ::drained nil))
         (.clear ~state-callbacks)
         (.clear ~cancellations)))))

(defmacro ensure-queue [state s]
  `(let [^NodeState s# ~s]
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

(defmacro read-from-queue [[lock state state-callbacks cancellations] forward queue-receive]
  (let [state-sym (gensym "state")]
    `(let [x# (l/with-exclusive-lock* ~lock
                (let [s# ~state]
                  (case (.mode s#)
                    (::split-one ::split-many)
                    ::split
                    
                    (ensure-queue state s#))))]

      (case x#
        ::split
        ~forward

        (let [~state-sym ^NodeState x#
              result# ~queue-receive]
          (when (instance? SuccessResult result#)
            (check-for-drained ~state ~state-callbacks ~cancellations))
          result#)))))

(deftype Node
  [^AsymmetricLock lock
   operator
   probe?
   ^{:volatile-mutable true :tag NodeState} state
   ^Map cancellations
   ^CopyOnWriteArrayList state-callbacks]

  l/LockProtocol

  (acquire [_] (l/acquire lock))
  (acquire-exclusive [_] (l/acquire-exclusive lock))
  (release [_] (l/release lock))
  (release-exclusive [_] (l/release-exclusive lock))
  (try-acquire [_] (l/try-acquire lock))
  (try-acquire-exclusive [_] (l/try-acquire-exclusive lock))

  PropagateProtocol

  ;;
  (downstream-nodes [_]
    (let [state state]
      (case (.mode state)
        (::one ::split-one)
        [(.next state)]

        (::many ::split-many)
        (seq (.next state))

        nil)))

  ;;
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

              (::drained ::closed)
              :lamina/closed!

              ::error
              :lamina/error!
              
              ::zero
              (enqueue-and-release lock state msg true)
              
              (::one ::split-one)
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
                            (case (.mode state)

                              (::one ::split-one)
                              (do
                                (enqueue-and-release (.lock node) state msg false)
                                (recur (.next state) msg))

                              (do
                                (l/release (.lock node))
                                (propagate node msg false))))))))))
              
              (::many ::split-many)
              (do
                (enqueue-and-release lock state msg false)

                ;; send message to all nodes
                (doseq [node (.next state)]
                  (propagate node msg true))

                :lamina/branch)))))))

  NodeProtocol

  ;;
  (state [_]
    state)

  ;;
  (ground [_]
    (let [x (l/with-exclusive-lock* lock
              (let [s state]
                (case (.mode s)

                  (::split-one ::split-many) ::split

                  (when-let [q (.queue state)]
                    (q/ground q)))))]
      (if (identical? ::split x)
        (ground (.split-next state))
        (do
          (check-for-drained state state-callbacks cancellations)
          x))))

  ;;
  (read-node [_]
    (if probe?
      (throw (IllegalStateException. "Cannot do single message consumption on probe-channels."))
      (read-from-queue [lock state state-callbacks cancellations]
       (read-node (.split-next state))
       (q/receive (.queue state)))))
  
  (read-node [_ predicate false-value result-channel]
    (if probe?
      (throw (IllegalStateException. "Cannot do single message consumption on probe-channels."))
      (read-from-queue [lock state state-callbacks cancellations]
       (read-node (.split-next state) predicate false-value result-channel)
       (q/receive (.queue state) predicate false-value result-channel))))

  ;;
  (receive [_ name callback]
    (if probe?
      (throw (IllegalStateException. "Cannot do single message consumption on probe-channels."))
      (let [x (l/with-exclusive-lock* lock
               (let [s state]
                 (case (.mode s)
                  
                   (::split-one ::split-many)
                   ::split
                  
                   (if-let [v (.get cancellations name)]
                    
                     (if (r/result-channel? v)
                       ::already-registered
                       ::invalid-name)
                    
                     ;; return the current state
                     (ensure-queue state s)))))]
       (case x
         ::split
         (receive (.split-next state) name callback)
        
         ::invalid-name
         (throw (IllegalStateException. "Invalid callback identifier used for (receive ...)"))

        
         ::already-registered
         true

         (let [^NodeState s x
               result (q/receive (.queue s) nil nil nil)]

           (cond

             (instance? SuccessResult result)
             (check-for-drained state state-callbacks cancellations)

             (instance? ResultChannel result)
             (when name
               (.put cancellations name
                 #(when-let [q (.queue ^NodeState (.state ^Node %))]
                    (q/cancel-receive q result)))
               (let [f (fn [_] (.remove cancellations name))]
                 (r/subscribe result (r/result-callback f f)))))

           (r/subscribe result (r/result-callback callback (fn [_]))))))))

  ;;
  (close [_]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (let [s state]
                   (case (.mode s)

                     (::closed ::drained ::error)
                     nil
                     
                     (close-node! state s))))]
        ;; signal state change
        (do
          (doseq [l state-callbacks]
            (l (.mode s) nil))
          (.clear state-callbacks)
          true)

        ;; state has already been permanently changed
        false)))

  ;;
  (error [_ err]
    (io! "Cannot modify node while in transaction."
      (if-let [old-queue (l/with-exclusive-lock* lock
                           (let [s state]
                             (case (.mode s)

                               (::drained ::error)
                               nil
                               
                               (let [q (.queue s)]
                                 (set-state! state s
                                   :mode ::error
                                   :queue (q/error-queue err)
                                   :error err
                                   :next nil)
                                 (or q ::nil-queue)))))]
        ;; signal state change
        (do
          (when-not (identical? ::nil-queue old-queue)
            (q/error old-queue err))
          (doseq [l state-callbacks]
            (l ::error err))
          (.clear state-callbacks)
          (.clear cancellations)
          true)
        
        ;; state has already been permanently changed
        false)))

  ;;
  (split [this]
    (let [n (l/with-exclusive-lock* lock
              (let [s state]

                (case (.mode s)

                  (::split-one ::split-many)
                  nil

                  (::closed ::drained ::error)
                  this

                  (::zero ::one ::many)
                  (let [n (Node.
                            (l/asymmetric-lock)
                            identity
                            probe?
                            s
                            (Collections/synchronizedMap (HashMap. cancellations))
                            (CopyOnWriteArrayList. state-callbacks))]
                    (.clear cancellations)
                    (.clear state-callbacks)
                    (set-state! state s
                      :mode ::split-one
                      :permanent? false
                      :consumed? false
                      :queue nil
                      :next n
                      :split-next n)
                    n))))]
      (on-state-changed n nil (siphon-callback this n))
      (on-state-changed this nil (join-callback n))
      (.put cancellations n #(unlink % n))
      n))
  
  ;;
  (link [this name node execute-within-lock]
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
                         
                           (::one ::split-one)
                           (set-state! state s
                             :mode (if (identical? ::one (.mode s))
                                     ::many
                                     ::split-many)
                             :next (CopyOnWriteArrayList. ^objects (to-array [(.next s) node])))
                         
                           (::split-many ::many)
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
                               :mode ::drained
                               :next node))
                         
                           (::error ::drained)
                           nil)]

                     (when new-state
                       (when execute-within-lock
                         (execute-within-lock))
                       (.put cancellations name #(unlink % node))
                       new-state))))]

        (do
          ;; if we've gone from ::zero -> ::one or ::closed -> ::drained,
          ;; send all queued messages
          (when-let [msgs (and
                            (case (.mode s)
                              (::one ::drained) true
                              false)
                            (.queue s)
                            (q/ground (.queue s)))]
            (doseq [msg msgs]
              (propagate (.next s) msg true)))

          ;; notify all state-changed listeners
          (doseq [l state-callbacks]
            (l (.mode ^NodeState s) nil))

          true)

        ;; already ::closed, ::drained, or ::error
        false)))

  ;;
  (unlink [_ node]
    (io! "Cannot modify node while in transaction."
      (if-let [s (l/with-exclusive-lock* lock
                   (let [s state]
                     (case (.mode s)
                       ::zero
                       nil
                       
                       (::one ::split-one)
                       (when (identical? node (.next s))
                         (if-not (.permanent? s)
                           (close-node! state s)
                           (set-state! state s
                             :mode ::zero
                             :queue (when (.consumed? s) (q/queue)))))
                       
                       (::many ::split-many)
                       (let [l ^CopyOnWriteArrayList (.next s)]
                         (when (.remove l node)
                           (if (identical? 1 (.size l))
                             (set-state! state s
                               :mode (if (identical? ::many (.mode s))
                                       ::one
                                       ::split-one)
                               :next (.get l 0))
                             ::state-unchanged)))
                           
                       (::closed ::drained :error)
                       nil)))]
        (do
          (when-not (identical? s ::state-unchanged)
            (doseq [l state-callbacks]
              (l (.mode ^NodeState s) nil)))
          true)
        false)))

  ;;
  (on-state-changed [_ name callback]
    (let [s (l/with-exclusive-lock lock
              (when (or (nil? name) (not (.containsKey cancellations name)))
                (let [s state]
                  (case (.mode s)

                    (::drained ::error)
                    nil
                    
                    (do
                      (.add state-callbacks callback)
                      (when name
                        (.put cancellations name
                          #(.remove ^CopyOnWriteArrayList (.state-callbacks ^Node %) callback)))))
                  s)))]
      (if s
        (do
          (callback (.mode ^NodeState s) (.error ^NodeState s))
          true)
        false)))

  ;;
  (cancel [this name]
    (if-let [x (l/with-lock lock
                 (case (.mode state)
                   (::split-one ::split-many)
                   (or (.remove cancellations name) ::split)

                   (.remove cancellations name)))]
      (cond
        (identical? ::split x)
        (cancel (.split-next state) name)

        (r/result-channel? x)
        (l/with-lock lock
          (q/cancel-receive (.queue state) x))

        :else
        (do
          (x this)
          true))

      ;; no such thing
      false)))

;;;

(defn closed? [node]
  (case (.mode (state node))
    (::closed ::drained) true
    false))

(defn drained? [node]
  (case (.mode (state node))
    ::drained true
    false))

(defn split? [node]
  (case (.mode (state node))
    (::split-one :split-many) true
    false))

(defn error-value [node default-value]
  (let [s (state node)]
    (case (.mode s)
      ::error (.error s)
      default-value)))

(defn queue [node]
  (.queue (state node)))

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
      (::closed ::drained ::error) (enqueue-cleanup #(cancel src dst))
      nil)))

(defn join-callback [dst]
  (fn [state err]
    (case state
      (::closed ::drained) (enqueue-cleanup #(close dst))
      ::error (enqueue-cleanup #(error dst err))
      nil)))

(defn siphon
  ([src dst]
     (siphon src dst nil))
  ([src dst execute-in-lock]
     (if (link src dst dst execute-in-lock)
       (do
         (on-state-changed dst nil (siphon-callback src dst))
         true)
       false)))

(defn join
  ([src dst]
     (join src dst nil))
  ([src dst execute-in-lock]
     (if (siphon src dst execute-in-lock)
       (do
         (on-state-changed src nil (join-callback dst))
         true)
       (cond
         (closed? src)
         (close dst)

         (not (identical? ::none (error-value src ::none)))
         (error dst (error-value src nil))

         :else
         false))))

;;;

(defn node
  ([operator]
     (node operator nil))
  ([operator messages]
     (when-not (ifn? operator)
       (throw (Exception. (str (pr-str operator) " is not a valid function."))))
     (Node.
       (l/asymmetric-lock)
       operator
       false
       (make-record NodeState
         :mode ::zero
         :queue (q/queue messages))
       (Collections/synchronizedMap (HashMap.))
       (CopyOnWriteArrayList.))))

(defn downstream-node [operator upstream-node]
  (let [n (node operator)]
    (join upstream-node n)
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

(defn on-error [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state err]
        (case state
          ::error
          (when (compare-and-set! latch false true)
            (callback err))

          nil)))))
