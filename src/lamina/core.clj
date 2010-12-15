;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns
  ^{:author "Zachary Tellman"}
  lamina.core
  (:use
    [potemkin :only (import-fn)])
  (:require
    [lamina.core.pipeline :as pipeline]
    [lamina.core.channel :as channel]
    [lamina.core.seq :as seq]))


;;;; CHANNELS

;; core channel functions
(import-fn channel/receive)
(import-fn channel/cancel-callback)
(import-fn channel/enqueue)
(import-fn channel/enqueue-and-close)
(import-fn channel/close)
(import-fn channel/on-close)
(import-fn channel/on-sealed)
(import-fn channel/sealed?)
(import-fn channel/closed?)
(import-fn channel/channel?)
(import-fn seq/receive-all)
(import-fn channel/poll)

;; channel variants
(import-fn channel/splice)
(import-fn channel/channel)
(import-fn channel/channel-pair)
(import-fn channel/constant-channel)
(import-fn channel/sealed-channel)

(def nil-channel channel/nil-channel)

;; channel utility functions

(import-fn seq/siphon)
(import-fn seq/fork)
(import-fn seq/map*)
(import-fn seq/filter*)
;;(import-fn channel/take*)
;;(import-fn channel/take-while*)

;; named channels
;;(import-fn channel/named-channel)
;;(import-fn channel/release-named-channel)

;; synchronous channel functions
(import-fn seq/lazy-channel-seq)
(import-fn seq/channel-seq)
(import-fn seq/wait-for-message)


;;;; PIPELINES

;; core pipeline functions
(import-fn pipeline/result-channel)
(import-fn pipeline/pipeline)
(import-fn pipeline/run-pipeline)

;; pipeline stage helpers
(import-fn pipeline/read-channel)
(import-fn pipeline/read-merge)
(import-fn pipeline/blocking)

;; redirect signals
(import-fn pipeline/redirect)
(import-fn pipeline/restart)
(import-fn pipeline/complete)

;; pipeline result hooks
(import-fn pipeline/wait-for-result)

;;;

(defn receive-in-order
  "Consumes messages from a channel one at a time.  The callback will only receive the next
   message once it has completed processing the previous one.

   This is a lossy iteration over the channel.  Fork the channel if there is another consumer."
  [ch f]
  (if (closed? ch)
    (pipeline/success-result nil)
    (run-pipeline ch
      read-channel
      (fn [msg]
	(f msg)
	(when-not (closed? ch)
	  (restart))))))

(defn- reduce- [f val ch]
  (:success
    (run-pipeline val
      (read-merge
	#(read-channel ch)
	#(if (and (nil? %2) (closed? ch))
	   %1
	   (f %1 %2)))
      (fn [val]
	(if (closed? ch)
	  val
	  (restart val))))))

(defn reduce*
  "Returns a constant-channel which will return the result of the reduce once the channel has been exhausted."
  ([f ch]
     (let [ch (fork ch)]
       (:success
	 (run-pipeline ch
	   read-channel
	   #(reduce- f %1 ch)
	   read-channel))))
  ([f val ch]
     (let [ch (fork ch)]
       (reduce- f val ch))))

(defn reductions- [f val ch]
  (let [ch* (channel)]
    (run-pipeline val
      (read-merge
	#(read-channel ch)
	#(if (and (nil? %2) (closed? ch))
	   %1
	   (f %1 %2)))
      (fn [val]
	(if (closed? ch)
	  (enqueue-and-close ch* val)
	  (do
	    (enqueue ch* val)
	    (restart val)))))
    ch*))

(defn reductions*
  "Returns a channel which contains the intermediate results of the reduce operation."
  ([f ch]
     (let [ch (fork ch)]
       (wait-for-message
	 (:success
	   (run-pipeline ch
	     read-channel
	     #(reductions- f %1 ch))))))
  ([f val ch]
     (let [ch (fork ch)]
       (reductions- f val ch))))
