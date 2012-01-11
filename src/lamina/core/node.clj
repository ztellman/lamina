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
    [lamina.core.lock :as l]
    [clojure.tools.logging :as log])
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

(defprotocol PropagateProtocol
  (downstream [_]
    "Returns a list of nodes which are downstream of this node.")
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed."))

(defprotocol NodeProtocol
  ;;
  (read-node [_] [_ predicate false-value result-channel]
    "Straight call to the queue which cannot be cancelled. Intended for (read-channel ...).")
  (receive [_ name callback]
    "Cancellable call to the queue. Intended for (receive ...).")

  ;;
  (close [_]
    "Closes the node. This is a permanent state change.")
  (error [_ error]
    "Puts the node into an error mode. This is a permanent state change.")

  ;;
  (link [_ name node execute-within-lock]
    "Adds a downstream node. The link should be cancelled using (cancel this name).")
  (unlink [_ node]
    "Removes a downstream node. Returns true if there was any such downstream node, false
     otherwise.")
  (consume [_ name node]
    )
  (split [_]
    "Splits a node so that the upstream half can be forked.")
  (cancel [_ name]
    "Cancels a callback registered by link, on-state-changed, or receive.  Returns true
     if a callback with that identifier is currently registered.")

  ;;
  (^NodeState state [_]
    "Returns the NodeState for this node.")
  (on-state-changed [_ name callback]
    "Adds a callback which takes two paramters: [state downstream-node-count error].
     Possible states are ::open, ::consumed, ::split, ::closed, ::drained, ::error.")
  (ground [_]
    "Consumes and returns all messages currently in the queue."))

;; The possible modes:
;; open
;; split
;; consumed     single consuming process (i.e. take-while*), messages go in queue
;; closed       no further messages can propagate through the node, messages still in queue
;; drained      no further messages can propagate through the node, queue is empty
;; error        an error prevents any further messages from being propagated
(deftype NodeState
  [mode
   ^long downstream-count
   split
   error
   ;; queue info
   queue 
   read?
   ;; special node states
   transactional?
   permanent?])

(defmacro set-state! [state state-val & key-vals]
  `(let [val# (assoc-record ~state-val ~@key-vals)]
     (set! ~state val#)
     val#))

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
           (log/warn e# "Error in map*/filter* function.")
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
           (l# ::drained 0 nil))
         (.clear ~state-callbacks)
         (.clear ~cancellations)))))

(defmacro ensure-queue [state s]
  `(let [^NodeState s# ~s]
     (if (.queue s#)
       s#
       (set-state! ~state s#
         :queue (q/queue)
         :read? true))))

(defmacro close-node! [state downstream-nodes s]
  `(let [^NodeState s# ~s
         q# (q/closed-copy (.queue s#))
         drained?# (q/drained? q#)]
     (.clear ~downstream-nodes)
     (set-state! ~state s#
       :mode (if drained?# ::drained ::closed)
       :queue (if drained?# (q/drained-queue) q#))))

(defmacro read-from-queue [[lock state state-callbacks cancellations] forward queue-receive]
  (let [state-sym (gensym "state")]
    `(let [x# (l/with-exclusive-lock* ~lock
                (let [s# ~state]
                  (if (identical? ::split (.mode s#))
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

(declare split-node join)

(deftype Node
  [^AsymmetricLock lock
   operator
   probe?
   ^{:volatile-mutable true :tag NodeState} state
   ^Map cancellations
   ^CopyOnWriteArrayList downstream-nodes
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
  (downstream [_]
    (seq downstream-nodes))

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

              ::consumed
              (enqueue-and-release lock state msg true)

              (::split ::open)
              (case (.downstream-count state)
                0
                (when-not probe?
                  (enqueue-and-release lock state msg true))

                1
                (do
                  (enqueue-and-release lock state msg false)
                  
                  ;; walk the chain of nodes until there's a split
                  (loop [node (.get downstream-nodes 0), msg msg]
                    
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

                          (::drained ::closed)
                          :lamina/closed
                          
                          (do
                            (l/acquire (.lock node))
                            (let [^NodeState state (.state node)]
                              (if (identical? 1 (.downstream-count state))
                                (do
                                  (enqueue-and-release (.lock node) state msg false)
                                  (recur
                                    (.get ^CopyOnWriteArrayList (.downstream-nodes node) 0)
                                    msg))
                                (do
                                  (l/release (.lock node))
                                  (propagate node msg false))))))))))

                ;; more than one node
                (do
                  (enqueue-and-release lock state msg false)
                  
                  ;; send message to all nodes
                  (doseq [n (downstream this)]
                    (propagate n msg true))
                  
                  :lamina/branch))))))))

  NodeProtocol

  ;;
  (state [_]
    state)

  ;;
  (ground [_]
    (let [x (l/with-exclusive-lock* lock
              (let [s state]
                (if (identical? ::split (.mode s))
                  ::split
                  (when-let [q (.queue state)]
                    (q/ground q)))))]
      (if (identical? ::split x)
        (ground (.split state))
        (do
          (check-for-drained state state-callbacks cancellations)
          x))))

  ;;
  (read-node [_]
    (read-from-queue [lock state state-callbacks cancellations]
      (read-node (.split state))
      (q/receive (.queue state))))
  
  (read-node [_ predicate false-value result-channel]
    (read-from-queue [lock state state-callbacks cancellations]
      (read-node (.split state) predicate false-value result-channel)
      (q/receive (.queue state) predicate false-value result-channel)))

  ;;
  (receive [_ name callback]
    (let [x (l/with-exclusive-lock* lock
              (let [s state]
                (if (identical? ::split (.mode s))
                  ::split
                  (if-let [v (.get cancellations name)]
                    
                    (if (r/result-channel? v)
                      ::already-registered
                      ::invalid-name)
                    
                    ;; return the current state
                    (ensure-queue state s)))))]
      (case x
        ::split
        (receive (.split state) name callback)
        
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

          (r/subscribe result (r/result-callback callback (fn [_])))))))

  ;;
  (close [_]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (let [s state]
                   (case (.mode s)

                     (::closed ::drained ::error)
                     nil
                     
                     (when-not (.permanent? s)
                       (close-node! state downstream-nodes s)))))]
        ;; signal state change
        (do
          (doseq [l state-callbacks]
            (l (.mode s) (.downstream-count s) nil))
          (.clear state-callbacks)
          true)

        ;; state has already been permanently changed or cannot be changed
        false)))

  ;;
  (error [_ err]
    (io! "Cannot modify node while in transaction."
      (if-let [old-queue (l/with-exclusive-lock* lock
                           (let [s state]
                             (case (.mode s)

                               (::drained ::error)
                               nil
                               
                               (when-not (.permanent? s)
                                 (let [q (.queue s)]
                                   (set-state! state s
                                     :mode ::error
                                     :queue (q/error-queue err)
                                     :error err)
                                   (or q ::nil-queue))))))]
        ;; signal state change
        (do
          (when-not (identical? ::nil-queue old-queue)
            (q/error old-queue err))
          (doseq [l state-callbacks]
            (l ::error 0 err))
          (.clear state-callbacks)
          (.clear cancellations)
          true)
        
        ;; state has already been permanently changed or cannot be changed
        false)))

  ;;
  (consume [this name node]
    (l/with-exclusive-lock* lock
      (let [s state]
        (if (identical? 0 (.downstream-count s))
          (do
            (.add downstream-nodes node)
            (.put cancellations name #(unlink % name))
            (set-state! state s
              :mode ::consumed
              :downstream-count 1)
            true)
          false))))

  ;;
  (split [this]
    (let [n (l/with-exclusive-lock* lock
              (let [s state]

                (case (.mode s)

                  ::split
                  nil

                  (::closed ::drained ::error)
                  this

                  (::open ::consumed)
                  (let [n (split-node this)]
                    (.clear cancellations)
                    (.clear state-callbacks)
                    (set-state! state s
                      :mode ::split
                      :read? false
                      :queue nil
                      :split n)
                    (.clear downstream-nodes)
                    (join this n)
                    n))))]
      (if (nil? n)
        (.split state)
        n)))
  
  ;;
  (link [this name node execute-within-lock]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (when-not (.containsKey cancellations name)
                   (let [s state
                         new-state
                         (case (.mode s)

                           (::open ::split)
                           (let [cnt (unchecked-inc (.downstream-count s))]
                             (.add downstream-nodes node)
                             (set-state! state s
                               :queue (when (.read? s) (q/queue))
                               :downstream-count cnt)
                             (assoc-record s
                               :downstream-count cnt))

                           ::closed
                           (do
                             (set-state! state s
                               :mode ::drained
                               :queue (q/drained-queue))
                             ;; make sure we return the original queue
                             (assoc-record s
                               :downstream-count 1
                               :mode ::drained))
                         
                           (::error ::drained ::consumed)
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
                              (::open ::drained) true
                              false)
                            (identical? 1 (.downstream-count s))
                            (.queue s)
                            (q/ground (.queue s)))]
            (doseq [msg msgs]
              (propagate node msg true)))

          ;; notify all state-changed listeners
          (let [^NodeState s s]
            (doseq [l state-callbacks]
              (l (.mode s) (.downstream-count s) nil)))

          true)

        ;; already ::closed, ::drained, or ::error
        false)))

  ;;
  (unlink [_ node]
    (io! "Cannot modify node while in transaction."
      (if-let [s (l/with-exclusive-lock* lock
                   (let [s state]
                     (case (.mode s)

                       (::open ::consumed ::split)
                       (when (.remove downstream-nodes node)
                         (let [cnt (unchecked-dec (.downstream-count s))]
                           (if (identical? 0 cnt)

                             ;; no more downstream nodes
                             (if-not (.permanent? s)
                               (close-node! state downstream-nodes s)
                               (set-state! state s
                                 :mode ::open
                                 :queue (or (.queue s) (q/queue))
                                 :split nil))

                             ;; reduce counter by one
                             (set-state! state s
                               :downstream-count cnt))))
                       
                       (::closed ::drained :error)
                       nil)))]
        (do
          (when-not (identical? s ::state-unchanged)
            (let [^NodeState s s]
              (doseq [l state-callbacks]
                (l (.mode s) (.downstream-count s) nil))))
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
        (let [^NodeState s s]
          (callback (.mode s) (.downstream-count s) (.error s))
          true)
        false)))

  ;;
  (cancel [this name]
    (if-let [x (l/with-lock lock
                 (if (identical? ::split (.mode state))
                   (or (.remove cancellations name) ::split)
                   (.remove cancellations name)))]
      (cond
        (identical? ::split x)
        (cancel (.split state) name)

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
  (identical? ::drained (.mode (state node))))

(defn split? [node]
  (identical? ::split (.mode (state node))))

(defn error-value [node default-value]
  (let [s (state node)]
    (if (identical? ::error (.mode s))
      (.error s)
      default-value)))

(defn queue [node]
  (.queue (state node)))

;;;

(deftype CallbackNode [callback]
  PropagateProtocol
  (downstream [_]
    nil)
  (propagate [_ msg _]
    (try
      (callback msg)
      (catch Exception e
        (log/error e "Error in permanent callback.")))))

(defn callback-node [callback]
  (CallbackNode. callback))

(deftype BridgeNode [callback downstream]
  PropagateProtocol
  (downstream [_]
    downstream)
  (propagate [_ msg _]
    (callback msg)))

(defn bridge-node [callback downstream]
  (BridgeNode. callback downstream))

;;;

(defn predicate-operator [predicate]
  #(if (predicate %)
     %
     ::false))

;;;

(defn siphon-callback [src dst]
  (fn [state _ _]
    (case state
      (::closed ::drained ::error) (enqueue-cleanup #(cancel src dst))
      nil)))

(defn join-callback [dst]
  (fn [state _ err]
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
     (node operator false nil))
  ([operator probe?]
     (node operator probe? nil))
  ([operator probe? messages]
     (when-not (ifn? operator)
       (throw (Exception. (str (pr-str operator) " is not a valid function."))))
     (Node.
       (l/asymmetric-lock)
       operator
       probe?
       (make-record NodeState
         :mode ::open
         :downstream-count 0
         :queue (q/queue messages))
       (Collections/synchronizedMap (HashMap.))
       (CopyOnWriteArrayList.)
       (CopyOnWriteArrayList.))))

(defn split-node
  [^Node node]
  (let [^NodeState s (.state node)]
    (Node.
      (l/asymmetric-lock)
      identity
      false
      (assoc-record s :permanent? false)
      (Collections/synchronizedMap (HashMap. ^Map (.cancellations node)))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.state-callbacks node))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.downstream-nodes node)))))

(defn downstream-node [operator ^Node upstream-node]
  (let [^Node n (node operator false)]
    (join upstream-node n)
    n))

(defn node? [x]
  (instance? Node x))

;;;

(defn walk-nodes [f exclusive? n]
  (let [s (downstream n)]
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
    (comp seq downstream)
    downstream
    n))

;;;

(defn on-closed [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state _ _]
        (case state
          (::closed ::drained)
          (when (compare-and-set! latch false true)
            (callback))

          nil)))))

(defn on-drained [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state _ _]
        (case state
          ::drained
          (when (compare-and-set! latch false true)
            (callback))

          nil)))))

(defn on-error [node callback]
  (let [latch (atom false)]
    (on-state-changed node callback
      (fn [state _ err]
        (case state
          ::error
          (when (compare-and-set! latch false true)
            (callback err))

          nil)))))
