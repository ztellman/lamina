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
    [lamina core]
    [lamina.trace.probe :only (probe-channel probe-enabled?)])
  (:require
    [lamina.core.context :as context]
    [lamina.executor.utils :as ex]
    [lamina.trace.timer :as t]))

(defrecord+ Enter [name ^long timestamp args])

(defmethod print-method Enter [x writer]
  (print-method (into {} x) writer))

;;;

(defmacro expand-multi-arities [instrument-macro f options-map]
  `(fn
     ([]
        (~instrument-macro ~options-map (~f) [])) 
     ([a#]
        (~instrument-macro ~options-map (~f a#) [a#]))
     ([a# b#]
        (~instrument-macro ~options-map (~f a# b#) [a# b#]))
     ([a# b# c#]
        (~instrument-macro ~options-map (~f a# b# c#) [a# b# c#]))
     ([a# b# c# d#]
        (~instrument-macro ~options-map (~f a# b# c# d#) [a# b# c# d#]))
     ([a# b# c# d# e#]
        (~instrument-macro ~options-map (~f a# b# c# d# e#) [a# b# c# d# e#]))
     ([a# b# c# d# e# ~'& rest#]
        (~instrument-macro ~options-map (apply ~f a# b# c# d# e# rest#) (list* a# b# c# d# e# rest#)))))

(defmacro task-fn-body
  [{:keys [nm
           capture
           executor
           enter-probe
           error-probe
           return-probe
           implicit?
           with-bindings?
           timeout]}
   invoke
   args]
  `(do
     (when (probe-enabled? ~enter-probe)
       (enqueue ~enter-probe (Enter. ~nm (System/currentTimeMillis) ~args)))
     (let [executor# ~executor
           timer# (t/enqueued-timer
                    ~executor
                    :capture ~capture
                    :name ~nm
                    :args ~args
                    :error-probe ~error-probe
                    :return-probe ~return-probe
                    :implicit? ~implicit?)]
       (ex/execute executor# timer#
         (~(if with-bindings? `lamina.core.utils/fast-bound-fn `fn) [] ~invoke)
         ~(when timeout `(when ~timeout (~timeout ~args)))))))

(defmacro fn-body
  [{:keys [nm
           capture
           enter-probe
           error-probe
           return-probe
           implicit?]}
   invoke
   args]
  `(do
     (when (probe-enabled? ~enter-probe)
       (enqueue ~enter-probe (Enter. ~nm (System/currentTimeMillis) ~args)))
     (let [timer# (t/timer
                    :capture ~capture
                    :name ~nm
                    :args ~args
                    :error-probe ~error-probe
                    :return-probe ~return-probe
                    :implicit? ~implicit?)]
       
       (try
         (let [result# (context/with-context (context/assoc-context :timer timer#)
                         ~invoke)]
           (when (async-result? result#)
             (t/mark-waiting timer#))
           (run-pipeline result#
             {:error-handler (fn [err#] (t/mark-error timer# err#))}
             (fn [x#] (t/mark-return timer# x#)))
           result#)
         (catch Exception e#
           (t/mark-error timer# e#)
           (throw e#))))))

(defn instrument-task-fn
  [f
   {:keys [executor capture timeout implicit? with-bindings? enter-probe error-probe return-probe]
    :as options
    :or {implicit? true
         capture :in-out
         with-bindings? false}}]
  (expand-multi-arities task-fn-body f
    {:name nm
     :capture capture
     :executor executor
     :enter-probe enter-probe
     :error-probe error-probe
     :return-probe return-probe
     :implict? implicit?
     :with-bindings? false
     :timeout timeout}))

(defn bound-instrument-task-fn
  [f
   {:keys [executor capture timeout implicit? with-bindings? enter-probe error-probe return-probe]
    :as options
    :or {implicit? true
         capture :in-out
         with-bindings? false}}]
  (expand-multi-arities task-fn-body f
    {:name nm
     :capture capture
     :executor executor
     :enter-probe enter-probe
     :error-probe error-probe
     :return-probe return-probe
     :implict? implicit?
     :with-bindings? true
     :timeout timeout}))

(defn instrument-fn
  [f
   {:keys [capture timeout implicit? with-bindings? enter-probe error-probe return-probe]
    :as options
    :or {implicit? true
         capture :in-out
         with-bindings? false}}]
  (expand-multi-arities fn-body f
    {:name nm
     :capture capture
     :enter-probe enter-probe
     :error-probe error-probe
     :return-probe return-probe
     :implicit? false
     :with-bindings? with-bindings?}))

;;;

(defn instrument
  "A general purpose transform for functions, allowing for tracing their execution,
   defining timeouts, and deferring their execution onto a thread pool.

   Instrumenting a function does not change its behavior in any way (unless an
   :executor is defined, see below).  This can be a powerful tool for both
   understanding complex tasks during development, and monitoring their behavior in
   production.

   ---------
   OVERHEAD

   Instrumenting adds some overhead to a function, equivalent to the performance
   difference between calling

     (+ 1 2 3)

   and

     (apply + [1 2 3])

   If you'd happily call 'apply' on the function being instrumented, chances are you
    won't notice the difference.

   ---------
   PROBES

   Instrumenting a function creates 'enter', 'return', and 'error' probes.  A :name
   must be specified, and probe names will be of the structure name:enter,
   name:return, etc.  Data emitted by these probes may be captured by other functions
   if :implicit? is set to true, which is the default.

   When the function is invoked, the 'enter' probe emits a hash of the form

     :name        - the :name specified in the options
     :timestamp   - time of invocation in milliseconds since the epoch
     :args        - a list of arguments passed to the function

   When the function completes and the value is realized, the 'return' probe
   will emit the data above, and also:

     :duration    - the time elapsed since the invocation, in nanoseconds
     :result      - the value returned by the function
     :sub-tasks   - 'return' probe data, less :result, for all implicit instrumented
     sub-functions

   If an error is thrown, or the value is realized as an error, :result is replaced by

     :error       - the exception thrown or realized error

   A :probes option may be defined, giving a hash of probe names onto channels that
   will consume their data:

     {:error (channel->> (sink #(println \"ERROR:\" %)))
      :return (channel->> (sink #(println \"Given\" (:args %) \", returned\" (:result %))))}

  ----------
  TIMEOUTS

  A :timeout option may be specified, which should be a function that takes the
  arguments passed to the function, and returns the timeout in milliseconds or nil
  for no timeout.  If the timeout elapses without any value, the returned result will
   be realized as an error of type 'lamina/timeout!'.

  ----------
  EXECUTORS

  If an :executor is specified, the function will be executed on that thread pool,
  and return an unrealized result representing its eventual value.

  In this case, :timeout will also interrupt the thread if it is still actively
  computing the value, and the 'return' probe will include an :enqueued-duration
  parameter that describes the time, in nanoseconds, spent waiting to be executed."
  
  [f {:keys [executor capture timeout probes implicit? with-bindings?]
      :as options
      :or {implicit? true
           capture :in-out
           with-bindings? false}}]
  (when-not (contains? options :name)
    (throw (IllegalArgumentException. "Instrumented functions must have a :name defined.")))
  (let [nm (name (:name options))
        enter-probe (probe-channel [nm :enter])
        return-probe (probe-channel [nm :return])
        error-probe (probe-channel [nm :error])]
    (doseq [[k v] probes]
      (siphon (probe-channel [~nm k]) v))
    (let [options (assoc options
                    :enter-probe enter-probe
                    :return-probe return-probe
                    :error-probe error-probe)]
      (if executor
        (if with-bindings?
          (bound-instrument-task-fn f options)
          (instrument-task-fn f options))
        (instrument-fn f options)))))

;;;

(defn transform-fn-bodies
  "Takes a (fn ...) form, and transform the bodies. The transform function is
   passed the arglist and the function body."
  [f form]
  (let [fn-form (macroexpand form)
        arity-forms (->> fn-form (drop-while symbol?))
        arity-forms (map
                      (fn [arity-form]
                        (let [args (first arity-form)]
                          `(~args ~@(f args (rest arity-form)))))
                      arity-forms)]
    `(~@(take-while symbol? fn-form)
      ~@arity-forms)))

(defmacro instrumented-fn
  "A compile-time version of (instrument ...).

   (instrumented-fn
     foo
     {:implicit? false}
     [x y]
     (+ x y))

   The "
  [fn-name
   {:keys
    [executor
     capture
     with-bindings?
     implicit?
     timeout
     probes]
    :or {capture :in-out
         with-bindings? false
         implicit? true}
    :as options}
   & fn-tail]
  (let [name (or (:name options)
               (str (-> (ns-name *ns*) str (.replace \. \:)) ":" (name fn-name)))]

    (when-not name
      (throw (IllegalArgumentException.
               "instrumented functions must either have a name, or have :name defined in the options")))

    (unify-gensyms
      `(let [enter-probe## (probe-channel [~name :enter])
             return-probe## (probe-channel [~name :return])
             error-probe## (probe-channel [~name :error])
             executor## ~executor
             capture## ~capture]
         (doseq [[k# v#] ~probes]
           (siphon (probe-channel [~name k#]) v#))
         ~(transform-fn-bodies
            (fn [args body]
              (let [args (vec (remove #{'&} args))]
                `((~(if executor
                      `task-fn-body
                      `fn-body)
                   {:name ~name
                    :capture capture##
                    :executor executor##
                    :enter-probe enter-probe##
                    :error-probe error-probe##
                    :return-probe return-probe##
                    :implicit? ~implicit?
                    :with-bindings? ~with-bindings?
                    :timeout ~timeout}
                   (do ~@body)
                   ~args))))
            `(fn ~@(when fn-name [fn-name]) ~@fn-tail))))))

(defmacro defn-instrumented
  "A def form of (instrumented-fn...). Options can be defined in the function metadata:

   (defn-instrumented foo
     {:implicit? false
      :timeout (constantly 1000)}
     \"Here's a doc-string\"
     [x]
     ...)

   The :name can be explicitly defined, but will default to

     the:function:namespace:the-function-name

   "
  [fn-name & body]
  (let [options (->> `(defn ~fn-name ~@body)
                  macroexpand
                  second
                  meta)]
    `(def ~fn-name
       (instrumented-fn ~fn-name ~options
         ~@(drop-while (complement sequential?) body)))))
