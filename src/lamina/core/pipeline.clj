;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.pipeline
  (:use
    [potemkin :only (unify-gensyms)]
    [lamina.core.utils :only (description)])
  (:require
    [lamina.trace.timer :as t]
    [lamina.trace.pipeline :as tr]
    [lamina.core.result :as r]
    [lamina.core.context :as context]
    [clojure.tools.logging :as log])
  (:import
    [lamina.trace.pipeline
     PipelineTracer]
    [lamina.core.result
     ResultChannel
     SuccessResult
     ErrorResult]))

(set! *warn-on-reflection* true)

;;;

(deftype Redirect [pipeline value])

(defprotocol IPipeline
  (gen-tracer [_ parent initial-value])
  (run [_ result initial-value value step])
  (trace-run [_ result initial-value value step tracer])
  (error [_ result initial-value ex tracer]))
 
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

(defn redirect-tracer [^PipelineTracer tracer pipeline value]
  (when tracer
    (let [tracer* (gen-tracer pipeline (.parent tracer) value)]
      (tr/mark-redirect tracer tracer*)
      tracer*)))

(defn resume-pipeline [pipeline result initial-value value step tracer]
  (loop [pipeline pipeline
         initial-value initial-value
         value value
         step step
         ^PipelineTracer tracer tracer]
    (let [val (if tracer
                (trace-run pipeline result initial-value value step tracer)
                (run pipeline result initial-value value step))]
      (if (instance? Redirect val)
        (let [^Redirect redirect val
              value (if (identical? ::initial (.value redirect))
                      initial-value
                      (.value redirect))
              pipeline (if (identical? ::current (.pipeline redirect))
                         pipeline
                         (.pipeline redirect))]
          (recur pipeline value value 0 (redirect-tracer tracer pipeline value)))
        val))))

(defn start-pipeline [pipeline result value tracer]
  (let [tracer (or tracer
                 (when-let [parent (context/pipeline-tracer)]
                   (gen-tracer pipeline parent value)))]
    (resume-pipeline pipeline result value value 0 tracer)))

(defn handle-redirect [^Redirect redirect result current-pipeline initial-value tracer]
  (let [value (if (identical? ::initial (.value redirect))
                initial-value
                (.value redirect))
        pipeline (if (identical? ::current (.pipeline redirect))
                   current-pipeline
                   (.pipeline redirect))]
    (start-pipeline pipeline result value (redirect-tracer tracer pipeline value))))

(defn subscribe [pipeline result initial-val val idx]
  (let [result (or result (r/result-channel))]
    (if-let [ctx (context/context)]
      (r/subscribe val
        (r/result-callback
          #(context/with-context ctx
             (resume-pipeline pipeline result initial-val % idx nil))
          #(context/with-context ctx
             (error pipeline result initial-val % nil))))
      (r/subscribe val
        (r/result-callback
          #(resume-pipeline pipeline result initial-val % idx nil)
          #(error pipeline result initial-val % nil))))
    result))

(defn trace-subscribe [pipeline result initial-val val tracer idx]
  (let [result (or result (r/result-channel))
        ctx (context/context)]
    (tr/mark-enter tracer idx)
    (r/subscribe val
      (r/result-callback
        #(context/with-context ctx
           (tr/mark-return tracer idx %)
           (resume-pipeline pipeline result initial-val % idx tracer))
        #(context/with-context ctx
           (tr/mark-error tracer idx %)
           (error pipeline result initial-val % tracer))))
    result))

;;;

(defn- split-options [opts+stages]
  (if (map? (first opts+stages))
    [(first opts+stages) (rest opts+stages)]
    [nil opts+stages]))

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
(defn- unwind-stages [idx stages remaining subscribe-expr]
  `(cond
       
     (r/result-channel? val##)
     (let [value# (r/success-value val## ::unrealized)]
       (if (identical? ::unrealized value#)
         ~(subscribe-expr idx)
         (recur value# (long ~idx))))
       

     (instance? Redirect val##)
     val##

     :else
     ~(if (empty? stages)
        `(if (identical? nil result##)
           (r/success-result val##)
           (do
             (r/success result## val##)
             result##))
        `(let [val## (~(first stages) val##)]
           ~(if (zero? remaining)
              `(recur val## (long ~(inc idx)))
              (unwind-stages
                (inc idx)
                (rest stages)
                (dec remaining)
                subscribe-expr))))))

;; totally ad hoc
(defn- max-depth [num-stages]
  (cond
    (< num-stages 4) 3
    (< num-stages 6) 2
    (< num-stages 8) 1
    :else 0))

(defn- complex-error-handler [error-handler]
  (if (= "pipeline" (str (first error-handler)))

    ;; pipeline error-handler
    (let [[opts stages] (split-options (rest error-handler))]
      (unify-gensyms
        `(error [this# result## initial-value# ex# tracer#]
           (let [result## (or result## (r/result-channel))
                 p# (pipeline
                      ~(merge opts {:result `result##})
                      ~@(butlast stages)
                      (fn [value#]
                        (let [value# (~(last stages) value#)]
                          (if (instance? Redirect value#)
                            (handle-redirect value# result## this# initial-value# tracer#)
                            (r/error-result ex#)))))]
             (p# ex#)
             result##))))

    ;; function error-handler
    `(error [this# result# initial-value# ex# tracer#]
       (let [value# (~error-handler ex#)]
         (if (instance? Redirect value#)
           (handle-redirect value# result# this# initial-value# tracer#)
           (if result#
             (r/error result# ex#)
             (r/error-result ex#)))))))

(defmacro pipeline [& opts+stages]
  (let [[options stages] (split-options opts+stages)
        {:keys [result error-handler]} options
        len (count stages)
        depth (max-depth len)]
    (unify-gensyms
      `(reify IPipeline

         (gen-tracer [_ parent# initial-value#]
           (tr/pipeline-tracer "pipeline" ~len initial-value# parent#))

         (trace-run [this## result## initial-val## val# step# tracer##]
           (if-not (or
                     (identical? nil result##)
                     (identical? nil (r/result result##)))
             (case (r/result result##)
               :success (tr/mark-complete tracer## (r/success-value result## nil))
               :error (tr/mark-error tracer## step# (r/error-value result## nil)))
             (let [cnt# (atom 0)]
               (try
                 (loop [val## val# step## (long step#)]
                   (reset! cnt# step##)
                   (if (= ~len step##)
                     (tr/mark-complete tracer## val##)
                     (tr/mark-stage tracer## step## val##))
                   (case (int step##)
                     ~@(interleave
                         (iterate inc 0)
                         (map
                           #(unwind-stages
                              %
                              (drop % stages)
                              0
                              (fn [idx] `(trace-subscribe this## result## initial-val## val## tracer## ~idx)))
                           (range (inc len))))))
                 (catch Exception ex#
                   (tr/mark-error tracer## @cnt# ex#)
                   (error this## result## initial-val## ex# tracer##))))))
         
         (run [this## result## initial-val## val# step#]
           (when (or
                   (identical? nil result##)
                   (identical? nil (r/result result##)))
             (try
               (loop [val## val# step## (long step#)]
                 (case (int step##)
                   ~@(interleave
                       (iterate inc 0)
                       (map
                         #(unwind-stages
                            %
                            (drop % stages)
                            depth
                            (fn [idx] `(subscribe this## result## initial-val## val## ~idx)))
                         (range (inc len))))))
               (catch Exception ex#
                 (error this## result## initial-val## ex# nil)))))
         
         ~(if error-handler
            (complex-error-handler error-handler)
            `(error [_# result# _# ex# _#]
               (when-not ~(contains? options :error-handler)
                 (log/error ex# "Unhandled exception in pipeline"))
               (if result#
                 (r/error result# ex#)
                 (r/error-result ex#))))

         clojure.lang.IFn
         (invoke [this# val#]
           (start-pipeline this# ~result val# nil))))))

(defmacro run-pipeline [value & opts+stages]
  `(let [p# (pipeline ~@opts+stages)
         value# ~value]
     (p# value#)))

;;;

(defn read-merge [read-fn merge-fn]
  (pipeline
    (fn [val]
      (run-pipeline (read-fn)
        #(merge-fn val %)))))

(defn complete [value]
  (redirect
    (pipeline (constantly value))
    nil))

(defmacro wait-stage [interval]
  `(fn [x#]
     (run-pipeline (r/timed-result ~interval)
       (fn [_#]
         x#))))


