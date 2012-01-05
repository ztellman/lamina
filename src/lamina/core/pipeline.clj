;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.pipeline
  (:require
    [lamina.core.result :as r])
  (:import
    [lamina.core.result
     ResultChannel
     SuccessResult
     ErrorResult]))

;;;

(deftype Redirect [pipeline value])

(defprotocol PipelineProtocol
  (run [_ result initial-value value step])
  (error [_ result ex]))

(defn redirect
  "If returned from a pipeline stage, redirects the pipeline flow to the beginning
   of 'pipeline', with an initial value of 'value'."
  [pipeline value]
  (Redirect. pipeline value))

(defn restart
  ([]
     (Redirect. ::current ::initial))
  ([value]
     (Redirect. ::current value)))

;;;

(defn start-pipeline [pipeline result initial-value value step]
  (loop [pipeline pipeline, initial-value initial-value, value value, step step]
    (let [val (run pipeline result initial-value value step)]
      (if (instance? Redirect val)
        (let [^Redirect redirect val
              value (if (= ::initial (.value redirect))
                      initial-value
                      (.value redirect))
              pipeline (if (= ::current (.pipeline redirect))
                         pipeline
                         (.pipeline redirect))]
          (recur pipeline value value 0))
        val))))

;;;

(defn- split-options [opts+stages]
  (let [s (partition-all 2 opts+stages)
        f (comp keyword? first)]
    [(->> s (take-while f) (apply concat) (apply hash-map))
     (->> s (drop-while f) (apply concat))]))

(defn- subscribe [this result initial-val val idx]
  `(let [result# (or ~result (r/result-channel))]
     (r/subscribe ~val
       (r/result-callback
         (fn [val#] (start-pipeline ~this result# ~initial-val val# ~idx))
         (fn [err#] (error ~this result# err#))))
     result#))

;; this is a Duff's device-ish unrolling of the pipeline, the resulting code
;; will end up looking something like:
;;
;; (case step
;;   0   (steps 0 to 2 and recur to 3)
;;   1   (steps 1 to 3 and recur to 4)
;;   ...
;;   n-2 (steps n-2 to n and return)
;;   n-1 (steps n-1 to n and return)
;;   n   (step n and return))
;;
;;  the longer the pipeline, the fewer steps per clause, since the JVM doesn't
;;  like big functions.  Currently at eight or more steps, each clause only handles
;;  a single step. 
(defn- unwind-stages [stages this result initial-val val idx remaining]
  `(cond
       
     (r/result-channel? ~val)
     (let [val# (r/success-value ~val ::unrealized)]
       (if (= ::unrealized val#)
         ~(subscribe this result initial-val val idx)
         (recur val# ~idx)))
       

     (instance? Redirect ~val)
     (let [^Redirect redirect# ~val]
       (if (= ::current (.pipeline redirect#))
         (let [value# (if (= ::initial (.value redirect#))
                        ~initial-val
                        (.value redirect#))]
           (recur value# 0))
         ~val))

     :else
     ~(if (empty? stages)
        `(if (= nil ~result)
           (r/success-result ~val)
           (r/success ~result ~val))
        (let [val-sym (gensym "val")]
          `(let [~val-sym (~(first stages) ~val)]
             ~(if (zero? remaining)
                `(recur ~val-sym ~(inc idx))
                (unwind-stages
                  (rest stages)
                  this
                  result
                  initial-val
                  val-sym
                  (inc idx)
                  (dec remaining))))))))

;; totally ad hoc
(defn- max-depth [num-stages]
  (cond
    (< num-stages 4) 3
    (< num-stages 6) 2
    (< num-stages 8) 1
    :else 0))

(defmacro pipeline [& opts+stages]
  (let [[options stages] (split-options opts+stages)
        len (count stages)
        depth (max-depth len)
        this (gensym "this")
        result (gensym "result")
        initial-val (gensym "initial-val")
        val (gensym "val")
        step (gensym "step")]
    `(reify PipelineProtocol
       (run [~this ~result ~initial-val ~val ~step]
         (when (or (= nil ~result) (= nil (r/result ~result)))
           (try
             (loop [~val ~val, ~step ~step]
               (case ~step
                 ~@(interleave
                     (iterate inc 0)
                     (map
                       #(unwind-stages
                          (drop % stages)
                          this
                          result
                          initial-val
                          val
                          %
                          depth)
                       (range (inc len))))))
             (catch Exception ex#
               (error ~this ~result ex#)))))
       (error [this# result# ex#]
         (if result#
           (r/error result# ex#)
           (r/error-result ex#)))
       clojure.lang.IFn
       (invoke [this# val#]
         (start-pipeline this# nil val# val# 0)))))

(defmacro run-pipeline [value & opts+stages]
  `(let [p# (pipeline ~@opts+stages)
         value# ~value]
     (start-pipeline p# nil value# value# 0)))


