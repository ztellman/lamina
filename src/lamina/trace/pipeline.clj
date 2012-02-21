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
  (current-stage [_])
  (mark-enter [_ step])
  (mark-return [_ step value])
  (mark-error [_ step error])
  (mark-redirect [_ timer])
  (add-sub-task [_ task]))

(deftype PipelineTracer
  [description
   channel
   timestamp
   ^objects stages
   ^ConcurrentLinkedQueue sub-tasks
   ^long parent-stage
   ^{:volatile-mutable true, :tag long} current-stage
   ^{:volatile-mutable true} next]

  clojure.lang.IDeref
  (deref [_]
    (merge
      {:description description
       :timestamp timestamp
       :stages (doall (map describe-stage-trace stages))
       :sub-tasks (doall (map deref sub-tasks))
       :next (when next @next)}
      (when (pos? parent-stage)
        {:parent-stage parent-stage})))
  
  IPipelineTracer

  (current-stage [_]
    current-stage)
  
  (mark-enter [this step]
    (when-not (aget stages step)
      (aset stages step
        (make-record StageTrace
          :timestamp (System/currentTimeMillis)
          :enter (System/nanoTime)
          :value ::none
          :error ::none
          :return Long/MIN_VALUE))
      (when channel
        (enqueue channel this))))

  (mark-return [this step value]
    (set! current-stage (long step))
    (let [^StageTrace trace (or (aget stages step)
                              (make-record StageTrace
                                :timestamp (System/currentTimeMillis)
                                :enter Long/MIN_VALUE
                                :return Long/MIN_VALUE
                                :value ::none
                                :error ::none))]
      (aset stages step
        (assoc-record trace
          :return (System/nanoTime)
          :value value)))
    (when channel
      (enqueue channel this)))

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
    (when channel
      (enqueue channel this)))

  (mark-redirect [_ timer]
    (set! next timer))
  
  (add-sub-task [_ task]
    (.add sub-tasks task)))

(defn root-pipeline-tracer [description channel]
  (PipelineTracer.
    description
    channel
    (System/currentTimeMillis)
    nil
    (ConcurrentLinkedQueue.)
    0
    0
    nil))

(defn pipeline-tracer
  [description num-steps parent]
  (let [tracer (PipelineTracer.
                 description
                 (context/pipeline-tracer-channel)
                 (System/currentTimeMillis)
                 (object-array (inc num-steps))
                 (ConcurrentLinkedQueue.)
                 (if parent
                   (current-stage parent)
                   0)
                 0
                 nil)]
    (when parent
      (add-sub-task parent tracer))
    tracer))
