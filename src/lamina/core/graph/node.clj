;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.graph.node
  (:use
    [potemkin]
    [flatland.useful.datatypes :only (make-record assoc-record)]
    [lamina.core.threads :only (enqueue-cleanup)]
    [lamina.core.graph.core]
    [lamina.core utils])
  (:require
    [lamina.core.result :as r]
    [lamina.core.queue :as q]
    [lamina.core.lock :as l]
    [lamina.core.threads :as t]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.lock
     AsymmetricLock]
    [lamina.core.graph.core
     Edge]
    [lamina.core.result
     SuccessResult
     ErrorResult
     ResultChannel]
    [java.util
     Collections
     HashMap
     Map]
    [java.util.concurrent.atomic
     AtomicBoolean]
    [java.util.concurrent
     CopyOnWriteArrayList]))


(defprotocol+ INode
  
  ;;
  (read-node [_] [_ predicate false-value result-channel]
    "Straight call to the queue which cannot be cancelled. Intended for (read-channel ...).")
  (receive [_ name callback]
    "Cancellable call to the queue. Intended for (receive ...).")

  ;;
  (link [_ name ^Edge edge pre-callback post-callback]
    "Adds a downstream node. The link should be cancelled using (cancel this name).")
  (unlink [_ ^Edge edge]
    "Removes a downstream node. Returns true if there was any such downstream node, false
     otherwise.")
  (consume [_ ^Edge edge]
    )
  (unconsume [_ edge]
    )
  (split [_]
    "Splits a node so that the upstream half can be forked.")
  (cancel [_ name]
    "Cancels a callback registered by link, on-state-changed, or receive.  Returns true
     if a callback with that identifier is currently registered.")

  ;;
  (^NodeState state [_]
    "Returns the NodeState for this node.")
  (set-state [_ val]
    )
  (on-state-changed [_ name callback]
    "Adds a callback which takes two paramters: [state downstream-node-count error].
     Possible states are ::open, ::consumed, ::split, ::closed, ::drained, ::error.")
  (drain [_]
    "Consumes and returns all messages currently in the queue."))

;;;

;; The possible modes:
;; open         messages can freely pass through
;; split        messages can freely pass through, queue operations are forwarded to upstream split node
;; consumed     single consuming process (i.e. take-while*), messages go in queue
;; closed       no further messages can propagate through the node, messages still in queue
;; drained      no further messages can propagate through the node, queue is empty
;; error        an error prevents any further messages from being propagated
(deftype+ NodeState
  [mode
   ^long downstream-count
   split
   error
   ;; queue info
   queue 
   ^boolean read?
   ;; special node states
   ^boolean transactional?
   ^boolean permanent?])

(defmacro set-state! [this state-val &
                      {:or {read? false
                            transactional? false
                            permanent? false}
                       :as key-vals}]
  `(let [val# (assoc-record ~(with-meta state-val {:tag NodeState}) ~@key-vals)]
     (set-state ~this val#)
     val#))

;;;

(defmacro enqueue-and-release [lock state msg persist?]
  `(if-let [q# (.queue ~state)]
     (q/enqueue q# ~msg ~persist? #(l/release ~lock))
     (l/release ~lock)))

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
           (error node# e# false)
           ::error)))))

(defmacro check-for-drained [this state watchers cancellations]
  `(let [^NodeState state# ~state]
     (when-let [q# (.queue state#)]
       (when (q/drained? q#)
         (set-state! ~this state#
           :mode ::drained
           :queue (q/drained-queue))
         (doseq [l# ~watchers]
           (l# ::drained 0 nil))
         (.clear ~watchers)
         (.clear ~cancellations)))))

(defmacro ensure-queue [this state s]
  `(let [^NodeState s# ~s]
     (if (.queue s#)
       s#
       (set-state! ~this s#
         :queue (if (.transactional? s#)
                  (q/transactional-queue)
                  (q/queue))
         :read? true))))

(defmacro close-node! [this edges s]
  `(let [^NodeState s# ~s
         q# (.queue s#)
         _# (when q# (q/close q#))
         drained?# (or (nil? q#) (q/drained? q#))]
     (.clear ~edges)
     (set-state! ~this s#
       :mode (if drained?# ::drained ::closed)
       :queue (if drained?# (q/drained-queue) q#))))

(defmacro read-from-queue [[this lock state watchers cancellations] forward queue-receive]
  (let [state-sym (gensym "state")]
    `(let [x# (l/with-exclusive-lock ~lock
                (let [s# ~state]
                  (if (identical? ::split (.mode s#))
                    ::split
                    (ensure-queue ~this ~state s#))))]

      (case x#
        ::split
        ~forward

        (let [~state-sym ^NodeState x#
              result# ~queue-receive]
          (when (instance? SuccessResult result#)
            (check-for-drained ~this ~state ~watchers ~cancellations))
          result#)))))

(defn drain-queue [^NodeState state]
  (when state
    (and
      (case (.mode state)
        (::open ::drained) true
        false)
      (= 1 (.downstream-count state))
      (.queue state)
      (q/drain (.queue state)))))

;;;

(declare split-node join node?)

(deftype Node
  [^AsymmetricLock lock
   operator
   description
   ^boolean grounded?
   ^{:volatile-mutable true :tag NodeState} state
   ^Map cancellations
   ^CopyOnWriteArrayList edges
   ^CopyOnWriteArrayList watchers]

  clojure.lang.Counted

  (count [_]
    (if-let [q (.queue state)]
      (count q)
      0))

  IDescribed
  (description [_] description)

  l/ILock
  (acquire [_] (l/acquire lock))
  (acquire-exclusive [_] (l/acquire-exclusive lock))
  (release [_] (l/release lock))
  (release-exclusive [_] (l/release-exclusive lock))
  (try-acquire [_] (l/try-acquire lock))
  (try-acquire-exclusive [_] (l/try-acquire-exclusive lock))

  IError
  
  (error [this err force?]
    (r/defer-within-transaction (error this err force?)
      (if-let [old-queue (l/with-exclusive-lock lock
                           (let [s state]
                             (case (.mode s)
                               
                               (::drained ::error)
                               nil
                               
                               (when-not (and (not force?) (.permanent? s))
                                 (let [q (.queue s)]
                                   (set-state! this s
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

  IPropagator

  ;;
  (downstream [_]
    (seq edges))

  ;;
  (propagate [this msg transform?]
    (if (and grounded? (= 0 (.downstream-count state)))
      :lamina/grounded
      (let [msg (transform-message this msg transform?)]
        (case msg

          ::error
          :lamina/error!
          
          :lamina/false
          :lamina/filtered
          
          (do
            ;; acquire the lock before we look at the state
            (l/acquire lock)
            (let [state state]
              (case (.mode state)

                (::drained ::closed)
                (do
                  (l/release lock)
                  :lamina/closed!)

                ::error
                (do
                  (l/release lock)
                  :lamina/error!)

                ::consumed
                (enqueue-and-release lock state msg true)

                (::split ::open)
                (case (.size edges)
                  0
                  (enqueue-and-release lock state msg (not grounded?))

                  1
                  (let [edge (.get edges 0)]
                    (enqueue-and-release lock state msg false)
                    
                    ;; walk the chain of nodes until there's a split
                    (loop [^Edge edge edge, msg msg]
                      (let [nxt (.next edge)]
                        (if-not (node? nxt)
                          
                          ;; if it's not a normal node, forward the message
                          (try
                            (propagate nxt msg true)
                            (catch Exception e
                              (error this e false)
                              :lamina/error!))
                          
                          (let [^Node node nxt
                                msg (transform-message node msg true)]
                            
                            (case msg
                              
                              ::error
                              :lamina/error!
                              
                              :lamina/false
                              :lamina/filtered

                              (do
                                (l/acquire (.lock node))
                                (let [^NodeState state (.state node)]
                                  (case (.mode state)

                                    (::drained ::closed)
                                    (do
                                      (l/release (.lock node))
                                      :lamina/closed!)
                              
                                    ::error
                                    (do
                                      (l/release (.lock node))
                                      :lamina/error!)

                                    ::consumed
                                    (enqueue-and-release (.lock node) state msg true)

                                    (::split ::open)
                                    (let [^CopyOnWriteArrayList edges (.edges node)]
                                      (if (= 1 (.size edges))
                                        (let [edge (.get edges 0)]
                                          (enqueue-and-release (.lock node) state msg false)
                                          (recur edge msg))
                                        (do
                                          (l/release (.lock node))
                                          (try
                                            (propagate node msg false)
                                            (catch Exception e
                                              (error this e false)
                                              :lamina/error!))))))))))))))

                  ;; more than one node
                  (do
                    (enqueue-and-release lock state msg false)
                    
                    ;; send message to all nodes
                    (try
                      (let [s (downstream this)
                            result (propagate (.next ^Edge (first s)) msg true)]
                        (doseq [^Edge e (rest s)]
                          (propagate (.next e) msg true))
                        result)
                      (catch Exception e
                        (error this e false)
                        :lamina/error!)))))))))))

  ;;
  (transactional [this]

    ;; most often this will already have been done, but a second acquisition is okay
    (l/acquire-exclusive this)
    (let [s state]
      (if (.transactional? s)

        ;; if we're already switched over, return
        (do
          (l/release-exclusive this)
          true)

        (do
          (set-state! this s
            :transactional? true
            :queue (q/transactional-copy (.queue s)))
          (let [downstream (downstream-propagators this)]

            ;; acquire children before releasing our own lock (hand-over-hand locking)
            ;; if we are interrupted while acquiring the children, make sure we release
            ;; our own lock
            (let [downstream-nodes (filter node? downstream)]
              (when (try
                      (l/acquire-all true downstream-nodes)
                      true
                      (finally
                        (l/release-exclusive this)))
                
                ;; lather, rinse, recur
                (try
                  (doseq [n downstream]
                    (transactional n))
                  (finally
                    (l/release-all true downstream-nodes)
                    true)))))))))

  INode

  ;;
  (state [_]
    state)

  ;;
  (drain [this]
    (let [x (l/with-exclusive-lock lock
              (let [s state]
                (if (identical? ::split (.mode s))
                  ::split
                  (.queue s))))]
      (if (identical? ::split x)
        (drain (.split state))
        (when x
          (let [msgs (q/drain x)]
            (check-for-drained this state watchers cancellations)
            msgs)))))

  ;;
  (read-node [this]
    (read-from-queue [this lock state watchers cancellations]
      (read-node (.split state))
      (q/receive (.queue state))))
  
  (read-node [this predicate false-value result-channel]
    (read-from-queue [this lock state watchers cancellations]
      (read-node (.split state) predicate false-value result-channel)
      (q/receive (.queue state) predicate false-value result-channel)))

  ;;
  (receive [this name callback]
    (let [x (l/with-exclusive-lock lock
              (let [s state]
                (if (identical? ::split (.mode s))
                  ::split
                  (if-let [v (.get cancellations name)]
                    
                    (if (r/async-result? v)
                      ::already-registered
                      ::invalid-name)
                    
                    ;; return the current state
                    (ensure-queue this state s)))))]
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
            (check-for-drained this state watchers cancellations)

            (instance? ResultChannel result)
            (when name
              (.put cancellations name
                #(when-let [q (.queue ^NodeState (.state ^Node %))]
                   (q/cancel-receive q result)))
              (let [f (fn [_] (.remove cancellations name))]
                (r/subscribe result (r/result-callback f f)))))

          (r/subscribe result (r/result-callback callback (fn [_])))))))

  ;;
  (close [this force?] 
    (r/defer-within-transaction (close this force?)
      (if-let [^NodeState s
               (l/with-exclusive-lock lock
                 (let [s state]
                   (case (.mode s)

                     (::closed ::drained ::error)
                     nil
                     
                     (when-not (and (not force?) (.permanent? s))
                       (close-node! this edges s)))))]
        ;; signal state change
        (do
          (when-not (check-for-drained this s watchers cancellations)
            (doseq [l watchers]
              (l (.mode s) (.downstream-count s) nil)))
          true)

        ;; state has already been permanently changed or cannot be changed
        false)))

  ;;
  (consume [this edge]
    (r/defer-within-transaction (consume this edge)
      (let [result
            (l/with-exclusive-lock lock
              (let [s state]
                (case (.mode s)
                  ::split
                  ::split

                  ::consumed
                  false

                  (::drained ::error)
                  true

                  (if-not (= 0 (.downstream-count s))
                    false
                    (do
                      (.add edges edge)
                      (set-state! this s
                        :mode ::consumed
                        :downstream-count 1)
                      (when (.transactional? s)
                        (transactional (.next ^Edge edge)))
                      true)))))]
        (if (identical? ::split result)
          (consume (.split state) edge)
          (when result
            (doseq [f watchers]
              (f ::consumed 1 nil))
            #(unconsume this edge ))))))

  ;;
  (unconsume [this edge]
    (r/defer-within-transaction (unconsume this edge)
      (let [result
            (l/with-exclusive-lock lock
              (let [s state]
                (case (.mode s)
                  ::split
                  ::split

                  ::consumed
                  (if-not (= edge (first edges))
                    false
                    (let [q (.queue s)]
                      (.clear edges)
                      (set-state! this s
                        :mode (if (q/closed? q)
                                ::closed
                                ::open)
                        :downstream-count 0)
                      (q/closed? q)))

                  false)))]
        (if (identical? ::split result)
          (unconsume (.split state) edge)
          (do
            (when result
              (doseq [f watchers]
                (f ::open 0 nil)))
            (boolean result))))))

  ;;
  (split [this]
    (l/with-exclusive-lock lock
      (let [s state]

        (case (.mode s)

          ::split
          (.split s)

          (::closed ::drained ::error)
          nil

          (::open ::consumed)
          (let [n (split-node this)]
            (.clear cancellations)
            (.clear watchers)
            (set-state! this s
              :mode ::split
              :read? false
              :downstream-count 0
              :queue nil
              :split n)
            (.clear edges)
            (join this (edge "split" n))
            n)))))

  ;;
  (link [this name edge pre post]
    (r/defer-within-transaction (link this name edge pre post)
      (if-let [^NodeState s
               (l/with-exclusive-lock lock
                 (when-not (.containsKey cancellations name)
                   (let [s state

                         ^NodeState new-state
                         (case (.mode s)

                           (::open ::split)
                           (let [cnt (if-not (sneaky-edge? edge)
                                       (unchecked-inc (.downstream-count s))
                                       (.downstream-count s))]
                             (.add edges edge)
                             (set-state! this s
                               :queue (when (.read? s)
                                        (if (.transactional? s)
                                          (q/transactional-queue)
                                          (q/queue)))
                               :downstream-count cnt)
                             (when (.transactional? s)
                               (transactional (.next ^Edge edge)))
                             (assoc-record ^NodeState s
                               :downstream-count cnt))

                           ::closed
                           (do
                             (set-state! this s
                               :mode ::drained
                               :queue (q/drained-queue))
                             ;; make sure we return the original queue
                             (assoc-record ^NodeState s
                               :downstream-count 1
                               :mode ::drained))
                         
                           (::error ::drained ::consumed)
                           nil)]

                     (when pre
                       (pre (boolean new-state)))

                     ;; if we've gone from ::zero -> ::one or ::closed -> ::drained,
                     ;; send all queued messages
                     (when-let [msgs (drain-queue new-state)]
                       (let [nxt (.next ^Edge edge)]
                         (doseq [msg msgs]
                           (propagate nxt msg true))))

                     (when new-state
                       (.put cancellations name #(unlink % edge)))
                     
                     (when post
                       (post (boolean new-state)))

                     new-state)))]

        (do
          ;; notify all state-changed listeners
          (let [^NodeState s s]
            (doseq [l watchers]
              (l (.mode s) (.downstream-count s) nil)))

          true)

        ;; already ::error, ::drained, ::consumed
        false)))

  ;;
  (unlink [this edge]
    (r/defer-within-transaction (unlink this edge)
      (if-let [s (l/with-exclusive-lock lock
                   (let [s state]
                     (case (.mode s)

                       (::open ::split)
                       (when (.remove edges edge)
                         (let [cnt (if-not (sneaky-edge? edge)
                                     (unchecked-dec (.downstream-count s))
                                     (.downstream-count s))]
                           (if (zero? cnt)

                             ;; no more downstream nodes
                             (if-not (.permanent? s)
                               (close-node! this edges s)
                               (set-state! this s
                                 :mode ::open
                                 :downstream-count cnt
                                 :queue (or (.queue s)
                                          (if (.transactional? s)
                                            (q/transactional-queue)
                                            (q/queue)))
                                 :split nil))

                             ;; reduce counter by one
                             (set-state! this s
                               :downstream-count cnt))))
                       
                       (::closed ::drained ::consumed ::error)
                       nil)))]
        (do
          (when-not (identical? s ::state-unchanged)
            (let [^NodeState s s]
              (doseq [l watchers]
                (l (.mode s) (.downstream-count s) nil))))
          true)
        false)))

  ;;
  (set-state [this val]
    (set! state val)
    val)

  ;;
  (on-state-changed [this name callback]
    (r/defer-within-transaction (on-state-changed this name callback)
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
        (when-let [^NodeState s s]
          (callback (.mode s) (.downstream-count s) (.error s)))
        (boolean s))))

  ;;
  (cancel [this name]
    (io! "Cannot cancel modifications to node within a transaction."
      (if-let [x (l/with-lock lock
                  (if (identical? ::split (.mode state))
                    (or (.remove cancellations name) ::split)
                    (.remove cancellations name)))]
       (cond
         (identical? ::split x)
         (cancel (.split state) name)

         (r/async-result? x)
         (l/with-lock lock
           (q/cancel-receive (.queue state) x))

         :else
         (do
           (x this)
           true))

       ;; no such thing
       false))))

(defn node? [x]
  (instance? Node x))

(defn closed? [node]
  (let [s (state node)]
    (boolean
      (or
        (case (.mode s)
          ::consumed (q/closed? (.queue s))
          (::closed ::drained ::error) true
          false)
        (and (.queue s) (q/closed? (.queue s)))))))

(defn split-node
  [^Node node]
  (let [^NodeState s (.state node)]
    (Node.
      (l/asymmetric-lock)
      identity
      nil
      (.grounded? node)
      (assoc-record ^NodeState s :permanent? false)
      (Collections/synchronizedMap (HashMap. ^Map (.cancellations node)))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.edges node))
      (CopyOnWriteArrayList. ^CopyOnWriteArrayList (.watchers node)))))

(defmacro node*
  [& {:keys [permanent? grounded? transactional? description operator messages]
      :or {operator identity
           description nil
           permanent? false
           grounded? false
           transactional? false
           messages nil}}]
  `(let [operator# ~operator]
     (when-not (ifn? operator#)
       (throw (Exception. (str (pr-str operator#) " is not a valid function."))))
     (Node.
       (l/asymmetric-lock)
       ~operator
       ~description
       ~grounded?
       (make-record NodeState
         :mode ::open
         :downstream-count 0
         :queue (let [messages# ~messages]
                  (if ~transactional?
                    (q/transactional-queue messages#)
                    (q/queue messages#)))
         :read? false
         :permanent? ~permanent?
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

;;;

(defn closed-result
  ([node]
     (closed-result node nil))
  ([node callback-name]
     (let [result (r/result-channel)]
       (on-state-changed node callback-name
         (fn [state _ err]
           (case state
             ::consumed
             (when (closed? node)
               (r/success result true))
          
             (::closed ::drained)
             (r/success result true)

             ::error
             (error result err false)
          
             nil)))
       result)))

(defn drained-result
  ([node]
     (drained-result node nil))
  ([node callback-name]
     (let [result (r/result-channel)]
       (on-state-changed node callback-name
         (fn [state _ err]
           (case state
             ::drained
             (r/success result true)

             ::error
             (error result err false)
          
             nil)))
       result)))

(defn error-result
  ([node]
     (error-result node nil))
  ([node callback-name]
     (let [result (r/result-channel)]
       (on-state-changed node callback-name
         (fn [state _ err]
           (case state
             ::error
             (error result err false)
          
             nil)))
       result)))

(defn error-value [node default-value]
  (let [s (state node)]
    (if (identical? ::error (.mode s))
      (.error s)
      default-value)))

(defn drained? [node]
  (let [s (state node)]
    (boolean
      (or
        (identical? ::drained (.mode s))
        (and (.queue s) (q/drained? (.queue s)))))))

(defn split? [node]
  (identical? ::split (.mode (state node))))

(defn consumed? [node]
  (identical? ::consumed (.mode (state node))))

(defn transactional? [node]
  (.transactional? (state node)))

(defn permanent? [node]
  (.permanent? (state node)))

(defn grounded? [node]
  (.grounded? ^Node node))

(defn queue [node]
  (.queue (state node)))

;;;

(defn upstream-callback [src dst join?]
  (fn [state _ err]
    (case state
      (::closed ::drained) (enqueue-cleanup #(cancel src dst))
      ::error (enqueue-cleanup (if join? #(error src err false) #(cancel src dst)))
      nil)))

(defn downstream-callback [src dst]
  (fn [state _ err]
    (case state
      ::drained (enqueue-cleanup #(close dst false))
      ::error (enqueue-cleanup #(error dst err false))
      nil)))

(defn siphon-cleanup-callback [callback dst]
  (fn [state _ _]
    (case state
      (::drained ::closed ::error) (cancel dst callback)
      nil)))

(defn connect
  ([src dst upstream? downstream?]
     (connect src dst upstream? downstream? nil nil))
  ([src dst upstream? downstream? pre post]
     (let [^Edge dst (if (edge? dst)
                       dst
                       (edge "connect" dst))]
       (link src (.next dst) dst
         pre
         (fn [success?]

           ;; upstream
           (when (and
                   success?
                   upstream?
                   (not (sneaky-edge? dst))
                   (node? (.next dst)))

             (let [callback (upstream-callback src (.next dst) true)]
               (on-state-changed (.next dst) callback
                 callback)
               (on-state-changed src nil
                 (siphon-cleanup-callback callback (.next dst)))))

           ;; downstream
           (when (and
                   success?
                   downstream?)
             (on-state-changed src nil
               (downstream-callback src (.next dst))))
           
           (when post
             (post success?)))))))

(defn siphon
  ([src dst]
     (siphon src dst nil nil))
  ([src dst pre post]
     (connect src
       (if (edge? dst)
         dst
         (edge "siphon" dst))
       true false pre post)))

(defn join
  ([src dst]
     (join src dst nil nil))
  ([src dst pre post]
     (connect src
       (if (edge? dst)
         dst
         (edge "join" dst))
       true true pre post)))
