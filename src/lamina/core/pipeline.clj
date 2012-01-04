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

(deftype UnrealizedResult [result])

(defprotocol PipelineProtocol
  (run [_ result initial-value value step])
  (error [_ result ex]))

;;;

(defn start-pipeline [pipeline result initial-value value step]
  (loop [pipeline pipeline, initial-value initial-value, result result, step step]
    (let [result (run pipeline result initial-value value step)]
      (if (= Redirect (class result))
        (let [^Redirect result result
              value (if (= ::initial (.value result))
                      initial-value
                      (.value result))]
          (recur (.pipeline result) value value 0))
        result))))

;;;

(defn split-options [opts+stages]
  (let [s (partition-all 2 opts+stages)
        f (comp keyword? first)]
    [(->> s (take-while f) (apply concat) (apply hash-map))
     (->> s (drop-while f) (apply concat))]))

(defmacro wrap-result [x]
  `(let [x# ~x]
     (if (r/result-channel? x#)
       x#
       (r/success-result x#))))

(defn subscribe [this result initial-val val idx]
  `(let [result# (or ~result (r/result-channel))]
     (r/subscribe ~val
       (r/result-callback
         (fn [val#] (start-pipeline ~this result# ~initial-val val# ~idx))
         (fn [err#] (error ~this result# err#))))
     result#))

(defn extract-result [val]
  (loop [val val]
    (let [val* (r/success-value val ::unrealized)]
      (if (= ::unrealized val*)
        (UnrealizedResult. val)
        (if-not (r/result-channel? val*)
          val*
          (recur val*))))))

(defn unwind-stages [stages this result initial-val val idx remaining]
  (let [val-sym (gensym "val")]
    `(cond
       
       (r/result-channel? ~val)
       (let [val# (extract-result ~val)]
         (if (= UnrealizedResult (class val#))
           (let [~val-sym (.result ^UnrealizedResult val#)]
             ~(subscribe this result initial-val val-sym idx))
           (recur val# ~idx)))
       

       (instance? Redirect ~val)
       ~val

       :else
       ~(if (empty? stages)
          `(if (= nil ~result)
             (r/success-result ~val)
             (r/success ~result ~val))
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
(defn max-depth [num-stages]
  (cond
    (< num-stages 4) 3
    (< num-stages 6) 2
    (< num-stages 8) 1
    :else 0))

(defmacro pipeline [& opts+stages]
  (let [[options stages] (split-options opts+stages)
        len (count stages)
        depth (max-depth len)
        this-sym (gensym "this")
        result-sym (gensym "result")
        initial-val-sym (gensym "initial-val")
        val-sym (gensym "val")
        step-sym (gensym "step")]
    `(reify PipelineProtocol
       (run [~this-sym ~result-sym ~initial-val-sym ~val-sym ~step-sym]
         (when (or (= nil ~result-sym) (= nil (r/result ~result-sym)))
           (try
             (loop [~val-sym ~val-sym, ~step-sym ~step-sym]
               (case ~step-sym
                 ~@(interleave
                     (iterate inc 0)
                     (map
                       #(unwind-stages
                          (drop % stages)
                          this-sym
                          result-sym
                          initial-val-sym
                          val-sym
                          %
                          depth)
                       (range (inc len))))))
             (catch Exception ex#
               (error ~this-sym ~result-sym ex#)))))
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


