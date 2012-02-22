;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.instrument
  (:use
    [potemkin]
    [lamina core])
  (:require
    [lamina.core.context :as context]
    [lamina.executor.utils :as ex]
    [lamina.trace.timer :as t]))

(defrecord Enter [^long timestamp args])

(defmacro instrument-task-body
  [nm executor enter-probe return-probe implicit? timeout invoke args]
  `(do
     (when (probe-enabled? ~enter-probe)
       (enqueue ~enter-probe (Enter. (System/currentTimeMillis) ~args)))
     (let [timer# (t/enqueued-timer ~nm ~args ~return-probe ~implicit?)]
       (ex/execute ~executor timer# (fn [] ~invoke) ~(when timeout `(when ~timeout (~timeout ~args)))))))

(defn instrument-task
  [f & {:keys [executor timeout implicit?]
        :as options
        :or {implicit? true}}]
  (let [nm (name (:name options))
        enter-probe (probe-channel [nm :enter])
        return-probe (probe-channel [nm :return])
        error-probe (probe-channel [nm :error])]
    (fn
      ([]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f) [])) 
      ([a]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f a) [a]))
      ([a b]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f a b) [a b]))
      ([a b c]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f a b c) [a b c]))
      ([a b c d]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f a b c d) [a b c d]))
      ([a b c d e] 
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (f a b c d e) [a b c d e]))
      ([a b c d e & rest]
         (instrument-task-body nm executor enter-probe return-probe implicit? timeout
           (apply f a b c d e rest) (list* a b c d e rest))))))

(defmacro instrument-body [nm enter-probe return-probe implicit? invoke args]
  `(do
     (when (probe-enabled? ~enter-probe)
       (enqueue ~enter-probe (Enter. (System/currentTimeMillis) ~args)))
     (let [timer# (t/timer ~nm ~args ~return-probe ~implicit?)]
       (context/with-context (context/assoc-context :timer timer#)
         (try
           (let [result# ~invoke]
             (run-pipeline result#
               {:error-handler (fn [err#] (t/mark-error timer# err#))}
               (fn [x#] (t/mark-return timer# x#)))
             result#)
           (catch Exception e#
             (t/mark-error timer# e#)
             (throw e#)))))))

(defn instrument [f & {:keys [executor timeout implicit?]
                       :as options
                       :or {implicit? true}}]
  (when-not (contains? options :name)
    (throw (IllegalArgumentException. "Instrumented functions must have a :name defined.")))
  (if executor
    (apply instrument-task f (apply concat options))
    (let [nm (name (:name options))
          enter-probe (probe-channel [nm :enter])
          return-probe (probe-channel [nm :return])
          error-probe (probe-channel [nm :error])]
      (doseq [[k v] (:probes options)]
        (siphon (probe-channel [~nm k]) v))
      (fn
        ([]
           (instrument-body nm enter-probe return-probe implicit?
             (f) []))
        ([a]
           (instrument-body nm enter-probe return-probe implicit?
             (f a) [a]))
        ([a b]
           (instrument-body nm enter-probe return-probe implicit?
             (f a b) [a b]))
        ([a b c]
           (instrument-body nm enter-probe return-probe implicit?
             (f a b c) [a b c]))
        ([a b c d]
           (instrument-body nm enter-probe return-probe implicit?
             (f a b c d) [a b c d]))
         ([a b c d e] 
           (instrument-body nm enter-probe return-probe implicit?
             (f a b c d e) [a b c d e]))
        ([a b c d e & rest]
           (instrument-body nm enter-probe return-probe implicit?
             (apply f a b c d e rest) (list* a b c d e rest)))))))

;;;

(defmacro defn-instrumented [fn-name & body]
  (let [form `(defn ~fn-name ~@body)
        form (macroexpand form)
        mta (merge
              {:implicit? true}
              (meta (second form)))
        nm (or (:name mta)
             (str (-> (ns-name *ns*) str (.replace \. \:)) ":" (name fn-name)))
        executor? (contains? mta :executor)
        implicit? (:implicit? mta)
        timeout (:timeout mta)]
    (unify-gensyms
      `(let [enter-probe## (probe-channel [~nm :enter])
             return-probe## (probe-channel [~nm :return])
             error-probe## (probe-channel [~nm :error])
             executor## ~(:executor mta)
             implicit?## ~(:implicit? mta)]
         (doseq [[k# v#] ~(:probes mta)]
           (siphon (probe-channel [~nm k#]) v#))
         ~(transform-defn-bodies
            (fn [args body]
              (if executor?
                `((instrument-task-body ~nm executor## enter-probe## return-probe## ~implicit? ~timeout
                    (do ~@body) ~args))
                `((instrument-body ~nm enter-probe## return-probe## ~implicit?
                    (do ~@body) ~args))))
            form)))))
