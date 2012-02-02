;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executor
  (:use
    [lamina.core]
    [potemkin :only (unify-gensyms)])
  (:import
    [java.util.concurrent
     LinkedBlockingQueue
     ThreadPoolExecutor
     ThreadFactory
     TimeUnit]))

(set! *warn-on-reflection* true)

(defprotocol IExecutor
  (execute [_ f] [_ f timeout])
  (shutdown [_]))

(defmacro executor [& {:keys [idle-timeout]
                       :as options}]
  (when-not (contains? options :name)
    (throw (IllegalArgumentException. "Must define :name for executor")))
  (unify-gensyms
    `(let [name# (name ~(:name options))
          timing-probe# (probe-channel [name# :timing])
          state-probe# (probe-channel [name# :state])
          cnt# (atom 0)
           pool# (ThreadPoolExecutor.
                   1
                   1
                   ~(or idle-timeout 60)
                   TimeUnit/SECONDS
                   (LinkedBlockingQueue.)
                   (reify ThreadFactory
                           (newThread [_ f#]
                             (doto
                               (Thread. f#)
                               (.setName (str name# "-" (swap! cnt# inc)))))))]
       (reify
         clojure.lang.IDeref
         (deref [_]
           {:completed-tasks (.getCompletedTaskCount pool#)
            :pending-tasks (- (.getTaskCount pool#) (.getCompletedTaskCount pool#))
            :active-threads (.getActiveCount pool#)
            :thread-count (.getPoolSize pool#)})
         IExecutor
         (shutdown [_]
           (.shutdown pool#))
         (execute [this# f#]
           (let [trace-state?# (probe-enabled? state-probe#)
                 trace-timing?# (probe-enabled? timing-probe#)
                 result# (result-channel)
                 f# (fn []
                      (run-pipeline nil
                        {:error-handler (fn [_#])
                         :result result#}
                        (fn [_#] (f#)))
                      (when trace-state?# (enqueue state-probe# @this#)))]
             (.execute pool# f#)
             (when trace-state?# (enqueue state-probe# @this#))
             result#))))))
