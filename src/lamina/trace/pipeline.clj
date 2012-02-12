;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.pipeline
  (:use
    [useful.datatypes :only (make-record assoc-record)]
    [lamina.core.utils :only (enqueue IEnqueue)])
  (:require
    [lamina.core.context :as context])
  (:import
    [java.util.concurrent
     ConcurrentLinkedQueue]))

(deftype StageTrace
  [^long timestamp
   ^long enter
   ^long return
   value
   error])

(defn describe-stage-trace [^StageTrace s]
  (when s
    (let [entered? (not= Long/MIN_VALUE (.enter s))
          returned? (not= Long/MIN_VALUE (.return s))
          value? (not= ::none (.value s))
          error? (not= ::none (.error s))]
      (merge
        {:timestamp (.timestamp s)}
        (when (and entered? returned?)
          {:duration (- (.return s) (.enter s))})
        (when (and entered? (not returned?))
          {:pending? true})
        (when value?
          {:value (.value s)})
        (when error?
          {:error (.error s)})))))

(defprotocol IPipelineTracer
  (mark-stage [_ step value])
  (mark-enter [_ step])
  (mark-return [_ step value])
  (mark-error [_ step error])
  (mark-redirect [_ timer])
  (mark-complete [_ value])
  (add-sub-task [_ task]))

(deftype PipelineTracer
  [parent
   description
   channel
   initial-value
   ^objects stages
   ^ConcurrentLinkedQueue sub-tasks
   ^{:volatile-mutable true} error
   ^{:volatile-mutable true} value
   ^{:volatile-mutable true} next]

  clojure.lang.IDeref
  (deref [_]
    (merge
      {:description description
       :initial-value initial-value
       :stages (doall (map describe-stage-trace stages))
       :sub-tasks (doall (map deref sub-tasks))
       :next (when next @next)}
      (when-not (= ::none value)
        {:value value})
      (when-not (= ::none error)
        {:error error})))
  
  IPipelineTracer
  
  (mark-stage [this step value]
    (when-not (aget stages step)
      (aset stages step
        (make-record StageTrace
          :timestamp (System/currentTimeMillis)
          :enter Long/MIN_VALUE
          :return Long/MIN_VALUE
          :value value
          :error ::none))
      (enqueue channel this)))

  (mark-enter [this step]
    (aset stages step
      (make-record StageTrace
        :timestamp (System/currentTimeMillis)
        :enter (System/nanoTime)
        :value ::none
        :error ::none
        :return Long/MIN_VALUE))
    (enqueue channel this))

  (mark-return [this step value]
    (let [^StageTrace trace (aget stages step)]
      (aset stages step
        (assoc-record trace
          :return (System/nanoTime)
          :value value)))
    (enqueue channel this))

  (mark-error [this step err]
    (let [^StageTrace trace (or (aget stages step)
                              (make-record StageTrace
                                :timestamp (System/currentTimeMillis)
                                :enter Long/MIN_VALUE
                                :return Long/MIN_VALUE))]
     (aset stages step
       (assoc-record trace
         :error err
         :return (System/nanoTime)
         :value ::none)))
    (set! error err)
    (enqueue channel this))

  (mark-redirect [_ timer]
    (set! next timer))
  
  (mark-complete [this val]
    (set! value val)
    (enqueue channel this))

  (add-sub-task [_ task]
    (.add sub-tasks task)))

(defn root-pipeline-tracer [description channel]
  (PipelineTracer.
    nil
    description
    channel
    nil
    nil
    (ConcurrentLinkedQueue.)
    ::none
    ::none
    nil))

(defn pipeline-tracer [description num-steps initial-value ^PipelineTracer parent]
  (let [tracer (PipelineTracer.
                 parent
                 description
                 (.channel parent)
                 initial-value
                 (object-array num-steps)
                 (ConcurrentLinkedQueue.)
                 ::none
                 ::none
                 nil)]
    (add-sub-task parent tracer)
    tracer))

;;;

(def dummy-channel
  (reify IEnqueue
    (enqueue [_ _])))

(defmacro trace-pipelines [& body]
  `(let [tracer# (root-pipeline-tracer nil dummy-channel)]
     (context/push-context :pipeline-tracer tracer#)
     (try
       ~@body
       (finally
         (context/pop-context)))
     (:sub-tasks @tracer#)))

