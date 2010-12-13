;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.seq
  (:use
    [lamina.core.channel]
    [lamina.core.observable :only (empty-value)]
    [lamina.core.queue :only (pop-)])
  (:import
    [java.util.concurrent TimeoutException]))

(defn lazy-channel-seq
  "Creates a lazy-seq which consumes messages from the channel.  Only elements
   which are realized will be consumes.

   (take 1 (lazy-channel-seq ch)) will only take a single message from the channel,
   and no more.  If there are no messages in the channel, execution will halt until
   a message is enqueued.

   'timeout' controls how long (in ms) the sequence will wait for each element.  If
   the timeout is exceeded or the channel is closed, the sequence will end.  By default,
   the sequence will never time out."
  ([ch]
     (lazy-channel-seq ch -1))
  ([ch timeout]
     (let [timeout-fn (if (fn? timeout)
			timeout
			(constantly timeout))]
       (lazy-seq
	 (when-not (closed? ch)
	   (let [value (promise)]
	     (receive (poll {:ch ch} (timeout-fn))
	       #(deliver value
		  (when (first %)
		    [(second %)])))
	     (let [val @value]
	       (when (and val
		       (or
			 (not (closed? ch))
			 (not (nil? (first val)))))
		 (concat val (lazy-channel-seq ch timeout-fn))))))))))

(defn channel-seq
  "Creates a non-lazy sequence which consumes all messages from the channel within the next
   'timeout' milliseconds.  A timeout of 0, which is the default, will only consume messages
   currently within the channel.

   This call is synchronous, and will hang the thread until the timeout is reached or the channel
   is closed."
  ([ch]
     (channel-seq ch 0))
  ([ch timeout]
     (doall
       (lazy-channel-seq ch
	 (if (neg? timeout)
	   (constantly timeout)
	   (let [t0 (System/currentTimeMillis)]
	     #(max 0 (- timeout (- (System/currentTimeMillis) t0)))))))))

(defn wait-for-message
  "Synchronously onsumes a single message from a channel.  If no message is received within the
   timeout, a java.util.concurrent.TimeoutException is thrown.  By default, this function will
   not time out."
  ([ch]
     (wait-for-message ch -1))
  ([ch timeout]
     (let [val (-> ch queue pop-)]
       (if-not (= empty-value val)
	 val
	 (let [result (promise)]
	  (receive (poll {:ch ch} timeout) #(deliver result %))
	  (if-let [result @result]
	    (do (println "result" result) (second result))
	    (throw (TimeoutException. "Timed out waiting for message from channel."))))))))
