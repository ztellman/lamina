;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
  lamina.core.timer
  (:import
    [java.util.concurrent
     ScheduledThreadPoolExecutor
     TimeUnit
     TimeoutException
     ThreadFactory]))

(def delayed-executor
  (ScheduledThreadPoolExecutor.
    (.availableProcessors (Runtime/getRuntime))
    (reify ThreadFactory
      (newThread [_ f]
        (doto (Thread. f)
          (.setDaemon true))))))

(defn delay-invoke [f delay]
  (.schedule
    ^ScheduledThreadPoolExecutor delayed-executor
    ^Runnable f
    (long delay) TimeUnit/MILLISECONDS))
