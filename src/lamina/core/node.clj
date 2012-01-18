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

(defprotocol IDescribed
  (description [_]))

(deftype Edge [^String description node]
  IDescribed
  (description [_] description))

(defn edge [description node]
  (Edge. description node))

(defn edge? [x]
  (instance? Edge x))

;;;

(defprotocol IPropagator
  (downstream [_]
    "Returns a list of nodes which are downstream of this node.")
  (transactional [_]
    "Makes this node and all downstream nodes transactional.")
  (propagate [_ msg transform?]
    "Sends a message downstream through the node. If 'transform?' is false, the node
     should treat the message as pre-transformed."))

(defn downstream-nodes [n]
  (map #(.node ^Edge %) (downstream n)))

;;;

(defprotocol INode
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
  (link [_ name ^Edge edge execute-within-lock]
    "Adds a downstream node. The link should be cancelled using (cancel this name).")
  (unlink [_ ^Edge edge]
    "Removes a downstream node. Returns true if there was any such downstream node, false
     otherwise.")
  (consume [_ name ^Edge edge]
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
;; open         messages can freely pass through
;; split        messages can freely pass through, queue operations are forwarded to upstream split node
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

(defmacro check-for-drained [state watchers cancellations]
  `(let [^NodeState state# ~state]
     (when-let [q# (.queue state#)]
       (when (q/drained? q#)
         (set-state! ~state state#
           :mode ::drained
           :queue (q/drained-queue))
         (doseq [l# ~watchers]
           (l# ::drained 0 nil))
         (.clear ~watchers)
         (.clear ~cancellations)))))

(defmacro ensure-queue [state s]
  `(let [^NodeState s# ~s]
     (if (.queue s#)
       s#
       (set-state! ~state s#
         :queue (if (.transactional? s#)
                  (q/transactional-queue)
                  (q/queue))
         :read? true))))

(defmacro close-node! [state edges s]
  `(let [^NodeState s# ~s
         q# (q/closed-copy (.queue s#))
         drained?# (q/drained? q#)]
     (.clear ~edges)
     (set-state! ~state s#
       :mode (if drained?# ::drained ::closed)
       :queue (if drained?# (q/drained-queue) q#))))

(defmacro read-from-queue [[lock state watchers cancellations] forward queue-receive]
  (let [state-sym (gensym "state")]
    `(let [x# (l/with-exclusive-lock* ~lock
                (let [s# ~state]
                  (if (identical? ::split (.mode s#))
                    ::split
                    (ensure-queue ~state s#))))]

      (case x#
        ::split
        ~forward

        (let [~state-sym ^NodeState x#
              result# ~queue-receive]
          (when (instance? SuccessResult result#)
            (check-for-drained ~state ~watchers ~cancellations))
          result#)))))

(declare split-node join node?)

(deftype Node
  [^AsymmetricLock lock
   operator
   description
   probe?
   ^{:volatile-mutable true :tag NodeState} state
   ^Map cancellations
   ^CopyOnWriteArrayList edges
   ^CopyOnWriteArrayList watchers]

  IDescribed
  (description [_] description)

  l/ILock
  (acquire [_] (l/acquire lock))
  (acquire-exclusive [_] (l/acquire-exclusive lock))
  (release [_] (l/release lock))
  (release-exclusive [_] (l/release-exclusive lock))
  (try-acquire [_] (l/try-acquire lock))
  (try-acquire-exclusive [_] (l/try-acquire-exclusive lock))

  IPropagator

  ;;
  (downstream [_]
    (seq edges))

  ;;
  (propagate [this msg transform?]
    (if (and probe? (= 0 (.downstream-count state)))
      :lamina/inactive-probe
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
                  (enqueue-and-release lock state msg (not probe?))

                  1
                  (do
                    (enqueue-and-release lock state msg false)
                    
                    ;; walk the chain of nodes until there's a split
                    (loop [^Edge edge (.get edges 0), msg msg]
                      (let [node (.node edge)]
                        (if-not (instance? Node node)
                          
                          ;; if it's not a normal node, forward the message
                          (try
                            (propagate node msg true)
                            (catch Exception e
                              (error this e)
                              :lamina/error!))
                          
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
                                        (.get ^CopyOnWriteArrayList (.edges node) 0)
                                        msg))
                                    (do
                                      (l/release (.lock node))
                                      (try
                                        (propagate node msg false)
                                        (catch Exception e
                                          (error this e)
                                          :lamina/error!))))))))))))

                  ;; more than one node
                  (do
                    (enqueue-and-release lock state msg false)
                    
                    ;; send message to all nodes
                    (try
                      (doseq [^Edge e (downstream this)]
                        (propagate (.node e) msg true))
                      :lamina/branch
                      (catch Exception e
                        (error this e)
                        :lamina/error!)))))))))))

  ;;
  (transactional [this]
    (io! "Cannot modify node while in transaction."

      ;; most often this will already have been done, but it's idempotent
      (l/acquire-exclusive this)
      (let [s state]
        (if (.transactional? s)

          ;; if we're already switched over, return
          true

          (do
            (set-state! state s
              :transactional? true
              :queue (q/transactional-copy (.queue s)))
            (let [downstream (downstream-nodes this)]

              ;; acquire children before releasing our own lock (hand-over-hand locking)
              ;; if we are interrupted while acquiring the children, make sure we release
              ;; our own lock
              (when (try
                      (->> downstream (filter node?) (l/acquire-all true))
                      true
                      (finally
                        (l/release-exclusive this)))

                ;; lather, rinse, recur
                (doseq [n downstream]
                  (transactional n))
                true)))))))

  INode

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
          (check-for-drained state watchers cancellations)
          x))))

  ;;
  (read-node [_]
    (read-from-queue [lock state watchers cancellations]
      (read-node (.split state))
      (q/receive (.queue state))))
  
  (read-node [_ predicate false-value result-channel]
    (read-from-queue [lock state watchers cancellations]
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
            (check-for-drained state watchers cancellations)

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
                       (close-node! state edges s)))))]
        ;; signal state change
        (do
          (doseq [l watchers]
            (l (.mode s) (.downstream-count s) nil))
          (.clear watchers)
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
                                   (.clear edges)
                                   (or q ::nil-queue))))))]
        ;; signal state change
        (do
          (when-not (identical? ::nil-queue old-queue)
            (q/error old-queue err))
          (doseq [l watchers]
            (l ::error 0 err))
          (.clear watchers)
          (.clear cancellations)
          true)
        
        ;; state has already been permanently changed or cannot be changed
        false)))

  ;;
  (consume [this name edge]
    (l/with-exclusive-lock* lock
      (let [s state]
        (if (identical? 0 (.downstream-count s))
          (do
            (.add edges edge)
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
                    (.clear watchers)
                    (set-state! state s
                      :mode ::split
                      :read? false
                      :queue nil
                      :split n)
                    (.clear edges)
                    (join this (edge "split" n))
                    n))))]
      (if (nil? n)
        (.split state)
        n)))

  ;;
  (link [this name edge execute-within-lock]
    (io! "Cannot modify node while in transaction."
      (if-let [^NodeState s
               (l/with-exclusive-lock* lock
                 (when-not (.containsKey cancellations name)
                   (let [s state
                         new-state
                         (case (.mode s)

                           (::open ::split)
                           (let [cnt (unchecked-inc (.downstream-count s))]
                             (.add edges edge)
                             (set-state! state s
                               :queue (when (.read? s)
                                        (if (.transactional? s)
                                          (q/transactional-queue)
                                          (q/queue)))
                               :downstream-count cnt)
                             (when (.transactional? s)
                               (transactional (.node ^Edge edge)))
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
                       (.put cancellations name #(unlink % edge))
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
            (let [node (.node ^Edge edge)]
              (doseq [msg msgs]
                (propagate node msg true))))

          ;; notify all state-changed listeners
          (let [^NodeState s s]
            (doseq [l watchers]
              (l (.mode s) (.downstream-count s) nil)))

          true)

        ;; already ::closed, ::drained, or ::error
        false)))

  ;;
  (unlink [_ edge]
    (io! "Cannot modify node while in transaction."
      (if-let [s (l/with-exclusive-lock* lock
                   (let [s state]
                     (case (.mode s)

                       (::open ::consumed ::split)
                       (when (.remove edges edge)
                         (let [cnt (unchecked-dec (.downstream-count s))]
                           (if (identical? 0 cnt)

                             ;; no more downstream nodes
                             (if-not (.permanent? s)
                               (close-node! state edges s)
                               (set-state! state s
                                 :mode ::open
                                 :queue (or (.queue s)
                                          (if (.transactional? s)
                                            (q/transactional-queue)
                                            (q/queue)))
                                 :split nil))

                             ;; reduce counter by one
                             (set-state! state s
                               :downstream-count cnt))))
                       
                       (::closed ::drained ::error)
                       nil)))]
        (do
          (when-not (identical? s ::state-unchanged)
            (let [^NodeState s s]
              (doseq [l watchers]
                (l (.mode s) (.downstream-count s) nil))))
          true)
        false)))

  ;;
  (on-state-changed [_ name callback]
    (let [callback (fn [& args]
                     (try
                       (apply callback args)
                       (catch Exception e
                         (log/error e "Error in on-state-changed callback."))))
          s (l/with-exclusive-lock lock
              (when (or (nil? name) (not (.containsKey cancellations name)))
                (let [s state]
                  (case (.mode s)

                    (::drained ::error)
                    nil
                    
                    (do
                      (.add watchers callback)
                      (when name
                        (.put cancellations name
                          #(.remove ^CopyOnWriteArrayList (.watchers ^Node %) callback)))))
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

(defn consumed? [node]
  (identical? ::consumed (.mode (state node))))

(defn error-value [node default-value]
  (let [s (state node)]
    (if (identical? ::error (.mode s))
      (.error s)
      default-value)))

(defn queue [node]
  (.queue (state node)))

;;;

(deftype CallbackNode [description callback]
  IDescribed
  (description [_] description)
  IPropagator
  (transactional [_] false)
  (downstream [_] nil)
  (propagate [_ msg _]
    (try
      (callback msg)
      (catch Exception e
        (log/error e "Error in permanent callback.")))))

(defn callback-node [callback]
  (CallbackNode. nil callback))

(defn callback-node? [n]
  (instance? CallbackNode n))

(deftype BridgeNode [description callback downstream]
  IDescribed
  (description [_] description)
  IPropagator
  (downstream [_] downstream)
  (propagate [_ msg _] (callback msg))
  (transactional [this]
    (doseq [n (downstream-nodes this)]
      (transactional n))))

(defn bridge-node [callback downstream]
  (BridgeNode. nil callback downstream))

(defn bridge-node? [n]
  (instance? BridgeNode n))

;;;

(defn predicate-operator [predicate]
  (with-meta
    (fn [x]
      (if (predicate x)
        x
        ::false))
    {::predicate predicate}))

(defn operator-predicate [f]
  (->> f meta ::predicate))

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
     (let [^Edge dst (if (edge? dst)
                       dst
                       (edge "siphon" dst))]
       (if (link src (.node dst) dst execute-in-lock)
         (do
           (on-state-changed (.node dst) nil (siphon-callback src (.node dst)))
           true)
         false))))

(defn join
  ([src dst]
     (join src dst nil))
  ([src dst execute-in-lock]
     (let [^Edge dst (if (edge? dst)
                       dst
                       (edge "join" dst))]
       (if (siphon src dst execute-in-lock)
         (do
           (on-state-changed src nil (join-callback (.node dst)))
           true)
         (cond
           (closed? src)
           (close dst)
           
           (not (identical? ::none (error-value src ::none)))
           (error (.node dst) (error-value src nil))
           
           :else
           false)))))

;;;

(defmacro node*
  [& {:keys [permanent? probe? transactional? description operator messages]
      :or {operator identity
           description nil
           permanent? false
           probe? false
           transactional? false
           messages nil}}]
  `(let [operator# ~operator]
     (when-not (ifn? operator#)
       (throw (Exception. (str (pr-str operator#) " is not a valid function."))))
     (Node.
       (l/asymmetric-lock)
       ~operator
       ~description
       ~probe?
       (make-record NodeState
         :mode ::open
         :downstream-count 0
         :queue ~(if transactional?
                   `(q/transactional-queue ~messages)
                   `(q/queue ~messages))
         :permanent? (or ~permanent? ~probe?)
         :transactional? ~transactional?)
       (Collections/synchronizedMap (HashMap.))
       (CopyOnWriteArrayList.)
       (CopyOnWriteArrayList.))))

(defn node
  ([operator]
     (node*
       :operator operator))
  ([operator messages]
     (node*
       :operator operator
       :messages messages)))

(defn split-node
  [^Node node]
  (let [^NodeState s (.state node)]
    (Node.
      (l/asymmetric-lock)
      identity
      nil
      false
      (assoc-record s :permanent? false)
      (Collections/synchronizedMap (HashMap. ^Map (.cancellations node)))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.edges node))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.watchers node)))))

(defn downstream-node [operator ^Node upstream-node]
  (let [^Node n (node operator)]
    (join upstream-node n)
    n))

(defn node? [x]
  (instance? Node x))

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
