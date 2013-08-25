;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.threads
  (:use
    [lamina.core.utils])
  (:import
    [java.util.concurrent
     ThreadFactory
     ThreadPoolExecutor
     TimeUnit
     LinkedBlockingQueue
     ScheduledThreadPoolExecutor]))

;;;

(let [cnt (atom 0)
      tf (thread-factory #(str "lamina-cleanup-" (swap! cnt inc)))]
  (def ^ThreadPoolExecutor cleanup-executor
    (ThreadPoolExecutor.
      (int (num-cores))
      Integer/MAX_VALUE
      (long 60)
      TimeUnit/SECONDS
      (LinkedBlockingQueue.)
      ^ThreadFactory tf)))

(def ^ThreadLocal cleanup-count (ThreadLocal.))

;; to prevent stack overflows
(def max-successive-cleanups 50)

(defn enqueue-cleanup [f]
  (let [cleanups (or (.get cleanup-count) 0)]
    (if (> cleanups max-successive-cleanups)
      (do
        (.execute cleanup-executor f)
        :lamina/deferred)
      (try
        (.set cleanup-count (inc cleanups))
        (f)
        (finally
          (when (zero? cleanups)
            (.set cleanup-count 0)))))))

;;;

