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
    [lamina.core.result :as r]
    [lamina.executor.utils :as ex]
    [lamina.core.context :as context]
    [clojure.tools.logging :as log])
  (:import
    [lamina.core.result
     ResultChannel
     SuccessResult
     ErrorResult]))

(set! *warn-on-reflection* true)

;;;

(deftype Redirect [pipeline value])

(defprotocol IPipeline
  (implicit? [_])
  (gen-timer [_ stage])
  (run [_ result initial-value value stage])
  (error [_ result initial-value ex]))
 
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

(defn resume-pipeline [pipeline result initial-value value stage]
  (loop [pipeline pipeline
         initial-value initial-value
         value value
         stage stage]
    
    (let [timer (gen-timer pipeline stage)
          val (run pipeline result initial-value value stage)]
      (when timer
        (t/mark-return timer nil))
      (if (instance? Redirect val)
        (let [^Redirect redirect val
              value (if (identical? ::initial (.value redirect))
                      initial-value
                      (.value redirect))
              pipeline (if (identical? ::current (.pipeline redirect))
                         pipeline
                         (.pipeline redirect))]
          (recur pipeline value value 0))
        val))))

(defn start-pipeline [pipeline result value]
  (resume-pipeline pipeline result value value 0))

(defn handle-redirect [^Redirect redirect result current-pipeline initial-value]
  (let [value (if (identical? ::initial (.value redirect))
                initial-value
                (.value redirect))
        pipeline (if (identical? ::current (.pipeline redirect))
                   current-pipeline
                   (.pipeline redirect))]

    ;; close out the current timer, if there is one
    (when (implicit? current-pipeline)
      (when-let [timer (context/timer)]
        (t/mark-return timer nil)))

    ;; start up the new pipeline
    (start-pipeline pipeline result value)))

(defn subscribe [pipeline result initial-val val fn-transform idx]
  (let [new-result? (nil? result)
        result (or result (r/result-channel))]
    (if-let [ctx (context/context)]

      ;; if we have context, propagate it forward
      (r/subscribe val
        (r/result-callback
          (fn-transform
            (fn [val]
              (context/with-context ctx
                (resume-pipeline pipeline result initial-val val idx))))
          (fn-transform
            (fn [err]
              (context/with-context ctx
                (error pipeline result initial-val err))))))
      
      ;; no context, so don't bother
      (r/subscribe val
        (r/result-callback
          (fn-transform
            (fn [val]
              (resume-pipeline pipeline result initial-val val idx)))
          (fn-transform
            (fn [err]
              (error pipeline result initial-val err))))))

    (if new-result?
      result
      :lamina/suspended)))

;;;

(defn- split-options [opts+stages form-meta]
  (cond
    (= :error-handler (first opts+stages))
    (do
      (println
        (str
          (ns-name *ns*) ", line " (:line form-meta) ": "
          "(pipeline :error-handler ...) is deprecated, use (pipeline {:error-handler ...} ...) instead."))
      [(apply hash-map (take 2 opts+stages)) (drop 2 opts+stages)])

    (map? (first opts+stages))
    [(first opts+stages) (rest opts+stages)]

    :else
    [nil opts+stages]))

;; this is a Duff's device-ish unrolling of the pipeline, the resulting code
;; will end up looking something like:
;;
;; (case stage
;;   0   (stages 0 to 2 and recur to 3)
;;   1   (stages 1 to 3 and recur to 4)
;;   ...
;;   n-2 (stages n-2 to n and return)
;;   n-1 (stages n-1 to n and return)
;;   n   (stage n and return))
;;
;;  the longer the pipeline, the fewer stages per clause, since the JVM doesn't
;;  like big functions.  Currently at eight or more stages, each clause only handles
;;  a single stage. 
(defn- unwind-stages [idx stages remaining subscribe-expr]
  `(cond
       
     (r/async-result? val##)
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

(defn- complex-error-handler [error-handler form-meta]
  (if (and (sequential? error-handler)
        (= "pipeline" (str (first error-handler))))

    ;; pipeline error-handler
    (let [[opts stages] (split-options (rest error-handler) form-meta)]
      (unify-gensyms
        `(error [this# result## initial-value# ex#]
           (let [result## (or result## (r/result-channel))
                 p# (pipeline
                      ~(merge {:error-handler `(fn [_#])} opts {:result `result##})
                      ~@(butlast stages)
                      (fn [value#]
                        (let [value# (~(last stages) value#)]
                          (if (instance? Redirect value#)
                            (handle-redirect value# result## this# initial-value#)
                            (r/error-result ex#)))))]
             (p# ex#)
             result##))))

    ;; function error-handler
    `(error [this# result# initial-value# ex#]
       (let [value# (~error-handler ex#)]
         (if (instance? Redirect value#)
           (handle-redirect value# result# this# initial-value#)
           (if result#
             (r/error result# ex#)
             (r/error-result ex#)))))))

(defmacro pipeline [& opts+stages]
  (let [[options stages] (split-options opts+stages (meta &form))
        {:keys [result
                error-handler
                timeout
                executor
                with-bindings?
                description
                implicit?]
         :or {description "pipeline"
              implicit? false}}
        options
        len (count stages)
        depth (max-depth len)
        location (str (ns-name *ns*) ", line " (:line (meta &form)))
        fn-transform (gensym "fn-transform")
        expand-subscribe (fn [idx] `(subscribe this## result## initial-val## val## ~fn-transform ~idx))]

    `(let [executor# ~executor
           fn-transform# identity
           fn-transform# (if executor#
                           (fn [f#] (fn [x#] (ex/execute executor# nil #(f# x#) ~(when implicit? `(context/timer)))))
                           fn-transform#)
           fn-transform# (if ~with-bindings?
                           (comp bound-fn* fn-transform#)
                           fn-transform#)
           ~fn-transform fn-transform#]
       (reify IPipeline

         (implicit? [_#]
           implicit?)

         ~(unify-gensyms
            `(gen-timer [_# stage##]
               ~(cond
                  (not implicit?) `nil
                  executor `(when (context/timer)
                              (if (neg? stage##)
                                (t/timer
                                  :description ~description
                                  :implicit? true)
                                (t/enqueued-timer
                                  :description ~description
                                  :implicit? true
                                  :start-stage stage##)))
                  :else `(when (context/timer)
                           (t/timer
                             :description ~description
                             :implicit? true
                             :start-stage stage##)))))
         
        ~(unify-gensyms
           `(run [this## result## initial-val## val# stage#]

              ;; if the result is already realized, stop now
              (when (or (identical? nil result##)
                      (identical? nil (r/result result##)))
                (try
                  ;; unwind the stages
                  (loop [val## val# stage## (long stage#)]
                    (case (int stage##)
                      ~@(interleave
                          (iterate inc 0)
                          (map
                            #(unwind-stages % (drop % stages) depth expand-subscribe)
                            (range (inc len))))))
                  (catch Exception ex#
                    (error this## result## initial-val## ex#))))))
       
        ~(if error-handler
           (complex-error-handler error-handler (meta &form))
           `(error [_# result# _# ex#]
              (when-not ~(contains? options :error-handler)
                (log/error ex# (str "Unhandled exception in pipeline at " ~location)))
              (if result#
                (r/error result# ex#)
                (r/error-result ex#))))

        clojure.lang.IFn
        ~(unify-gensyms
           `(invoke [this## val##]
              ~(cond
                 (and timeout (not result))
                 `(let [result# (r/expiring-result ~timeout)]
                    (start-pipeline this## result# val##)
                    result#)
                 
                (and result (not timeout))
                `(start-pipeline this## ~result val##)

                (and result timeout)
                `(let [result# ~result]
                   (on-success (r/timed-result ~timeout) (fn [_] (r/error result# :lamina/timeout!)))
                   (start-pipeline this## result# val##))

                :else
                `(start-pipeline this## nil val##))))))))

(defmacro run-pipeline [value & opts+stages]
  `(let [p# ~(with-meta `(pipeline ~@opts+stages) (meta &form))
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
     (r/timed-result ~interval x#)))


