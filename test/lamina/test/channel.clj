;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.channel
  (:use
    [lamina.core]
    [lamina.core.channel :only (listen)])
  (:use
    [clojure.test]
    [clojure.contrib.def]
    [clojure.walk]
    [clojure.contrib.combinatorics]))

;;

(defn async-enqueue [ch s slow?]
  (future
    (try
      (doseq [x s]
	(enqueue ch x)
	(if slow?
	  (Thread/sleep 1)
	  (Thread/sleep 0 1)))
      (close ch)
      (catch Exception e
	(.printStackTrace e)))))

(declare callback)

(defmacro output-of [f & body]
  `(let [coll# (atom [])]
     (binding [callback (fn [msg#] (swap! coll# conj (~f msg#)))]
       ~@body
       @coll#)))

;;;

(defn receive-in-order* [ch callback]
  (receive ch
    (fn this [msg]
      (callback msg)
      (when-not (drained? ch)
	(receive ch this)))))

(defmacro close-output= [expected ch & body]
  (let [ch-sym (gensym "ch")
	body (postwalk-replace {'ch ch-sym} body)]
    (let [f (fn [ch]
	      (fn [msg]
		[msg (drained? ch)]))]
      `(do
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym) (receive-all ~ch-sym callback) ~@body))))
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym) (receive-in-order* ~ch-sym callback) ~@body))))
	 (let [~ch-sym ~ch]
	   (is (= ~expected (output-of (~f ~ch-sym)
			      (receive ~ch-sym
				(fn this# [msg#]
				  (callback msg#)
				  (when-not (drained? ~ch-sym)
				    (receive ~ch-sym this#))))
			      ~@body))))))))

(deftest test-close
  (close-output= [[1 false] [2 true]]
    (closed-channel 1 2))
  (close-output= [[1 false] [2 false] [nil true]]
    (channel 1 2)
    (close ch))
  (close-output= [[1 false] [2 false] [nil true]]
    (channel)
    (enqueue-and-close ch 1 2))
  (close-output= [[1 false] [2 false] [3 false] [nil true]]
    (channel)
    (enqueue ch 1 2)
    (enqueue-and-close ch 3)))

(deftest test-siphon-sealing
  (let [a (channel), b (channel)]
    (siphon a b)
    (enqueue a 1 2 3)
    (is (= [1 2 3] (channel-seq b)))
    (close b)
    (is (closed? a)))

  (let [a (permanent-channel), b (channel)]
    (siphon a b)
    (enqueue a 1 2 3)
    (is (= [1 2 3] (channel-seq b)))
    (close b)
    (is (not (closed? a)))
    (let [c (channel)]
      (siphon a c)
      (enqueue a 1 2 3)
      (is (= [1 2 3] (channel-seq c)))))

  (let [a (channel)
	b (map* inc a)
	c (map* dec b)]
    (enqueue a 1 2 3)
    (is (empty? (channel-seq a)))
    (is (empty? (channel-seq b)))
    (is (= [1 2 3] (channel-seq c)))
    (close c)
    (is (closed? b))
    (is (closed? a)))

  (let [a (permanent-channel)
	b (map* inc a)
	c (map* dec b)]
    (enqueue a 1 2 3)
    (is (empty? (channel-seq a)))
    (is (empty? (channel-seq b)))
    (is (= [1 2 3] (channel-seq c)))
    (close c)
    (is (closed? b))
    (is (not (closed? a)))))


;;;

;; Register a series of listeners that only receive one value
(deftest test-simple-listen
  (let [ch (channel)
	coll (atom [])
	num 1e3]
    (async-enqueue ch (range num) true)
    (dotimes [_ num]
      (let [watch (atom false)
	    latch (promise)]
	(listen ch (fn [msg]
		     (when (compare-and-set! watch false true) 
		       [true (fn [msg]
			       (swap! coll conj msg)
			       (deliver latch nil))])))
	@latch))
    (is (= (range num) @coll))))

;; Register large number of listeners, but only let one receive each value
(deftest test-listen
  (let [ch (channel)
	coll (atom [])
	waiting-for (ref 0)
	num 1e3
	latch (promise)]
    (async-enqueue ch (range num) true)
    (while (< (count @coll) num)
      (listen ch (fn [msg]
		   (when (= msg (ensure waiting-for))
		     (alter waiting-for inc)
		     [true (fn [msg]
			     (swap! coll conj msg)
			     (when (= (dec num) msg)
			       (deliver latch nil)))]))))
    @latch
    (is (= (range num) @coll))))

(deftest test-on-closed
  (let [num 1e3
	cnt (atom 0)]
    (dotimes [i num]
      (let [ch (channel)]
	(future (close ch))
	(Thread/sleep (rand-int 10))
	(on-closed ch #(swap! cnt inc))))
    (Thread/sleep 20)
    (is (= @cnt num))))

;; polling

(deftest test-simple-poll
  (let [ch (channel)
	num 1e3]
    (let [coll (atom [])]
      (async-enqueue ch (range num) false)
      (dotimes [i num]
	(when-let [[ch msg] (wait-for-message (poll {:ch ch}))]
	  (swap! coll conj msg)))
      (is (= (range num) @coll)))))

(deftest test-poll
  (let [u (channel)
	v (channel)
	num 1e3]
    (let [colls {:u (atom [])
		 :v (atom [])}]
      (async-enqueue u (range num) false)
      (async-enqueue v (range num) false)
      (dotimes [i (+ 2 (* 2 num))]
	(when-let [[ch msg] (wait-for-message (poll {:u u, :v v}))]
	  (swap! (colls ch) conj msg)))
      (is (= (concat (range num) [nil]) @(:u colls)))
      (is (= (concat (range num) [nil]) @(:v colls))))))

(deftest test-poll-timeout
  (let [ch (channel)]
    (is (= nil (wait-for-message (poll {:ch ch} 0) 0)))))

;; synchronous methods

(deftest test-wait-for-message
  (let [num 1e2]
    (let [ch (channel)]
      (async-enqueue ch (range num) false)
      (dotimes [i num]
	(is (= i (wait-for-message ch 100)))))))

(deftest test-channel-seq
  (let [ch (channel 1)]
    (is (= [1] (channel-seq ch))))
  (let [ch (closed-channel 1 nil)]
    (is (= [1] (channel-seq ch))))

  (let [in (range 1e3)
	target (last in)
	ch (channel)]
    (async-enqueue ch in false)
    (is (= in
	   (loop [out []]
	     (Thread/sleep 0 1)
	     (let [n (channel-seq ch)]
	       (if-not (= target (last n))
		 (recur (concat out n))
		 (concat out n))))))))

;; fork

(deftest test-receive-all
  (dotimes [i 1e2]
    (let [result (atom [])]
      (let [s (range 10)]
	(let [ch (channel)]
	  (async-enqueue ch s false)
	  (Thread/sleep (rand-int 10))
	  (let [latch (promise)]
	    (receive-all ch #(when %
			       (when (= 10 (count (swap! result conj %)))
				 (deliver latch nil))))
	    @latch
	    (is (= @result s))))))))

(deftest test-fork
  (dotimes [i 1e2]
    (let [s (range 10)]
      (let [ch (channel)]
	(async-enqueue ch s false)
	(Thread/sleep (rand-int 10))
	(let [ch* (fork ch)]
	  (is (= s (lazy-channel-seq ch)))
	  (is (= s (lazy-channel-seq ch*))))))))

(deftest test-fork-receive-all
  (dotimes [i 1e2]
    (let [result (atom [])]
      (let [s (range 10)]
	(let [ch (channel)]
	  (async-enqueue ch s false)
	  (Thread/sleep (rand-int 10))
	  (let [ch* (fork ch)]
	    (let [latch (promise)]
	      (receive-all ch* #(when %
				  (when (= 10 (count (swap! result conj %)))
				    (deliver latch nil))))
	      @latch
	      (is (= @result s)))))))))

;; seq-like methods

(deftest test-take*
  (let [s (range 10)]
    (let [ch (apply closed-channel s)
	  ch* (take* 5 ch)]
      (is (= (range 5) (channel-seq ch*)))
      (is (drained? ch*))
      (is (= (range 5 10) (channel-seq ch)))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (take* 5 ch)]
      (async-enqueue ch s true)
      (is (= (range 5) (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (= (range 5 10) (channel-seq ch 2500)))
      (is (drained? ch)))

    (let [ch (apply closed-channel s)
	  ch* (take* 11 ch)]
      (is (= s (channel-seq ch*)))
      (is (drained? ch*))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (take* 11 ch)]
      (async-enqueue ch s true)
      (is (= s (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (drained? ch)))))

(deftest test-take-while*
  (let [s (range 10)]
    (let [ch (apply closed-channel s)
	  ch* (take-while* #(< % 5) ch)]
      (is (= (range 5) (channel-seq ch*)))
      (is (drained? ch*))
      (is (= (range 5 10) (channel-seq ch)))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (take-while* #(< % 5) ch)]
      (async-enqueue ch s true)
      (is (= (range 5) (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (= (range 5 10) (channel-seq ch 2500)))
      (is (drained? ch)))

    (let [ch (apply closed-channel s)
	  ch* (take-while* #(<= % 11) ch)]
      (is (= s (channel-seq ch*)))
      (is (drained? ch*))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (take-while* #(<= % 11) ch)]
      (async-enqueue ch s true)
      (is (= s (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (drained? ch)))))

(deftest test-mapcat*
  (let [s [1 2 3]
	f range]

    (let [ch (apply closed-channel s)
	  ch* (mapcat* f ch)]
      (is (= (mapcat f s) (channel-seq ch*)))
      (is (drained? ch))
      (is (drained? ch*)))

    (let [ch (channel)
	  ch* (mapcat* f ch)]
      (async-enqueue ch s true)
      (is (= (mapcat f s) (channel-seq ch* 2500)))
      (is (drained? ch))
      (is (drained? ch*)))))

(deftest test-map*
  (let [s (range 10)
	f #(* % 2)]

    (let [ch (apply closed-channel s)
	  ch* (map* f ch)]
      (is (= (map f s) (channel-seq ch*)))
      (is (drained? ch))
      (is (drained? ch*)))

    (let [ch (channel)
	  ch* (map* f ch)]
      (async-enqueue ch s true)
      (is (= (map f s) (channel-seq ch* 2500)))
      (is (drained? ch))
      (is (drained? ch*)))))

(deftest test-filter*
  (let [s (range 10)]

    (let [ch (apply closed-channel s)
	  ch* (filter* even? ch)]
      (is (= (filter even? s) (channel-seq ch*)))
      (is (drained? ch))
      (is (drained? ch*)))

    (let [ch (channel)
	  ch* (filter* even? ch)]
      (async-enqueue ch s true)
      (is (= (filter even? s) (channel-seq ch* 2500)))
      (is (drained? ch))
      (is (drained? ch*)))))

(deftest test-reduce*
  (let [s (range 10)]

    (let [ch (apply closed-channel s)]
      (is (= (reduce + s) @(reduce* + ch))))

    (let [ch (channel)]
      (async-enqueue ch s false)
      (is (= (reduce + s) (wait-for-result (reduce* + ch) 2500))))))

(deftest test-reductions*
  (let [s (range 10)]

    (let [ch (apply closed-channel s)
	  ch* (reductions* + ch)]
      (is (= (reductions + s) (channel-seq ch*)))
      (is (drained? ch*))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (reductions* + ch)]
      (async-enqueue ch s false)
      (is (= (reductions + s) (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (drained? ch)))))

(deftest test-partition*
  (let [s (range 10)]

    (let [ch (apply closed-channel s)
	  ch* (partition* 4 3 ch)]
      (is (= (partition 4 3 s) (channel-seq ch*)))
      (is (drained? ch*))
      (is (drained? ch)))

    (let [ch (channel)
	  ch* (partition* 4 3 ch)]
      (async-enqueue ch s false)
      (is (= (partition 4 3 s) (channel-seq ch* 2500)))
      (is (drained? ch*))
      (is (drained? ch)))))

(deftest test-partition-all*
  (let [s (range 10)]

    (let [ch (apply closed-channel s)
	  ch* (partition-all* 4 3 ch)]
      (is (= (partition-all 4 3 s) (channel-seq ch*)))
      (is (drained? ch))
      (is (drained? ch*)))

    (let [ch (channel)
	  ch* (partition-all* 4 3 ch)]
      (async-enqueue ch s false)
      (is (= (partition-all 4 3 s) (channel-seq ch* 2500)))
      (is (drained? ch))
      (is (drained? ch*)))))

;;;

;; TODO: reduce this down to a more atomic test
(defn- priority-compose-channels
  "Uses the order defined in the channels seq to fully consume the first channels
   messages before consuming the messages from the rest of the channels.  All results
   are enqueued into an output channel that is returned"
  ([channels channel-end-marker]
     (priority-compose-channels channels channel-end-marker (channel)))
  ([[fchan & rchans ] channel-end-marker output-channel]
     (if fchan
       (do
	 (siphon fchan output-channel)
         (on-drained fchan
	   (fn [] 
	     (enqueue output-channel channel-end-marker)
	     (priority-compose-channels rchans channel-end-marker output-channel))))
       (close output-channel))
     output-channel))

(deftest test-multi-channel-close
  (testing "Composing two channels with priority"
    (let [chan1 (channel) ;; Should have priority over chan2
          chan2 (channel)
          out-chan (priority-compose-channels [chan1 chan2] :done)]
      (enqueue chan1 [1 1])
      (enqueue chan2 [2 1])
      (enqueue chan1 [1 2])
      (enqueue chan2 [2 2])
      (close chan2)

      (is (= [[1 1] [1 2]] (channel-seq out-chan)))
      (close chan1)
      (is (= [:done [2 1] [2 2] :done] (channel-seq out-chan)))
      (is (every? closed? [chan1 chan2 out-chan])))))
