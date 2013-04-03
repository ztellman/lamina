;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.pipeline
  (:use
    [potemkin]
    [lamina.core.utils :only (description)])
  (:require
    [lamina.core.utils :as u]
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



;;;

(deftype+ Redirect [pipeline value])

(definterface+ IPipeline
  (implicit? [_])
  (gen-timer [_ stage])
  (run-finally [_])
  (run [_ result initial-value value stage])
  (error [_ result initial-value ex]))
 
(defn redirect
  "Returns a redirect signal which causes the flow to start at the beginning of `pipeline`, with an input
   value of `value`.  The outcome of this new `pipeline` will be forwarded into the result returned by the
   original pipeline."
  [pipeline value]
  (Redirect. pipeline value))

(defn restart
  "A variant of `redirect` which redirects flow to the top of the current pipeline."
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
                         (do
                           (run-finally pipeline)
                           (.pipeline redirect)))]
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
(defn- unwind-stages [idx stages remaining subscribe-expr unwrap?]
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
        `(do
           (run-finally this##)
           (if (identical? nil result##)
             ~(if unwrap?
                `val##
                `(r/success-result val##))
             (do
               (r/success result## val##)
               result##)))
        `(let [val## (~(first stages) val##)]
           ~(if (zero? remaining)
              `(recur val## (long ~(inc idx)))
              (unwind-stages
                (inc idx)
                (rest stages)
                (dec remaining)
                subscribe-expr
                unwrap?))))))

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
           (run-finally this#)
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
       (run-finally this#)
       (let [value# (~error-handler ex#)]
         (if (instance? Redirect value#)
           (handle-redirect value# result# this# initial-value#)
           (if result#
             (u/error result# ex# false)
             (r/error-result ex#)))))))

(defmacro pipeline
  "A means for composing asynchronous functions.  Returns a function which will pass the value into the first
   function, the result from that function into the second, and so on.

   If any function returns an unrealized async-result, the next function won't be called until that value is realized.
   The call into the pipeline itself returns an async-result, which won't be realized until all functions have completed.
   If any function throws an exception or returns an async-result that realizes as an error, this will short-circuit all
   calls to subsequent functions, and cause the pipeline's result to be realized as an error.

   Loops and other more complex flows may be created if any stage returns a redirect signal by returning the result of 
   invoking `restart`, `redirect`, or `complete`.  See these functions for more details.

   The first argument to `pipeline` may be a map of optional arguments:

     `:error-handler` - a function which is called when an error occurs in the pipeline.  Takes a single argument, the `error`,
                        and may optionally return a redirect signal to prevent the pipeline from returning the error.

                        If no `:error-handler` is specified, the error will be logged.  If pipelines are nested, this may result
                        in the same error being logged multiple times.  To hide this error you may define a no-op handler, but
                        only do this if you're sure there's an outer pipeline that will handle/log the error.

     `:finally` - a function wich is called with zero arguments when the pipeline completes, either due to success or error.

     `:result` - the result into which the pipeline's result will be forwarded.  Causes the pipeline to not return any value.

     `:timeout` - the max duration of the pipeline's invocation.  If pipeline times out in the middle of a stage it won't terminate
                  computation, but it will not continue onto the next stage.

     `:implicit?` - describes whether the pipeline's execution should show up in higher-level instrumented functions calling into it.
                    Defaults to false.

     `:unwrap?` - if true, and the pipeline does not need to pause between streams, the pipeline will return an actual value 
                  rather than an async-result.

     `:with-bindings` - if true, conveys the binding context of the initial invocation of the pipeline into any deferred stages."
  [& opts+stages]
  (let [[options stages] (split-options opts+stages (meta &form))
        {:keys [result
                error-handler
                timeout
                executor
                with-bindings?
                middleware
                description
                implicit?
                unwrap?
                finally
                flow-exceptions]
         :or {description "pipeline"
              implicit? false
              unwrap? false}}
        options
        len (count stages)
        depth (max-depth len)
        location (str (ns-name *ns*) ", line " (:line (meta &form)))
        fn-transform (gensym "fn-transform")
        expand-subscribe (fn [idx] `(subscribe this## result## initial-val## val## ~fn-transform ~idx))]

    ;; handle compositions of 
    `(let [executor# ~executor
           fn-transform# (or ~middleware identity) 
           fn-transform# (if executor#
                           (fn [f#]
                             (fn [x#]
                               (ex/execute executor# nil
                                 #(f# x#)
                                 ~(when implicit? `(context/timer)))))
                           fn-transform#)
           fn-transform# (if ~with-bindings?
                           (comp potemkin/fast-bound-fn* fn-transform#)
                           fn-transform#)
           ~fn-transform fn-transform#]
       (reify lamina.core.pipeline.IPipeline

         (implicit? [_#]
           ~implicit?)

         (run-finally [_#]
           ~@(when finally
               `((try*
                   (~finally)
                   (catch Throwable e#
                     (log/error e# "error in finally clause"))))))

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
                            #(unwind-stages % (drop % stages) depth expand-subscribe unwrap?)
                            (range (inc len))))))
                  ~@(map
                      (fn [ex] `(catch ~ex ex# (throw ex#)))
                      (distinct flow-exceptions))
                  (catch Error ex#
                    (if-not (lamina.core.utils/retry-exception? ex#)
                      (error this## result## initial-val## ex#)
                      (throw ex#)))
                  (catch Throwable ex#
                    (error this## result## initial-val## ex#))))))
       
        ~(if error-handler
           (complex-error-handler error-handler (meta &form))
           `(error [this# result# _# ex#]
              (when-not ~(contains? options :error-handler)
                (log/error ex# (str "Unhandled exception in pipeline at " ~location)))
              (run-finally this#)
              (if result#
                (u/error result# ex# false)
                (r/error-result ex#))))

        clojure.lang.IFn
        ~(unify-gensyms
           `(invoke [this## val##]
              ~(cond
                 (and timeout (not result))
                 `(let [timeout# ~timeout
                        result# (if timeout#
                                  (r/expiring-result timeout#)
                                  (r/result-channel))]
                    (start-pipeline this## result# val##)
                    result#)
                 
                (and result (not timeout))
                `(start-pipeline this## ~result val##)

                (and result timeout)
                `(let [timeout# ~timeout
                       result# ~result]
                   (when timeout#
                     (on-success (r/timed-result timeout#)
                       (fn [_] (u/error result# :lamina/timeout! false))))
                   (start-pipeline this## result# val##))

                :else
                `(start-pipeline this## nil val##))))))))

(defmacro run-pipeline
  "Like `pipeline`, but simply invokes the pipeline with `value` and returns the result."
  [value & opts+stages]
  `(let [p# ~(with-meta `(pipeline ~@opts+stages) (meta &form))
         value# ~value]
     (p# value#)))

;;;

(defn complete
  "Returns a redirect signal which causes the pipeline's execution to stop, and simply return `value`.  If `value`
   is a `Throwable`, then the pipeline will be realized as that error."
  [value]
  (redirect
    (if (instance? Throwable value)
      (pipeline {:error-handler (fn [_])} (fn [_] (throw value)))
      (pipeline (constantly value)))
    nil))

(defmacro wait-stage
  "Creates a pipeline stage which simply waits for `interval` milliseconds before continuing onto the next stage."
  [interval]
  `(fn [x#]
     (r/timed-result ~interval x#)))


