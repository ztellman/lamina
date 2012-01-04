;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.threads
  (:import
    [java.util.concurrent
     ExecutorService
     Executors
     ThreadFactory]))

(set! *warn-on-reflection* true)

(defn thread-factory [name-generator]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. runnable) (.setName (name-generator))))))

(def ^ExecutorService cleanup-executor
  (Executors/newSingleThreadExecutor
    (thread-factory (constantly "Lamina - cleanup thread"))))

(defn enqueue-cleanup [f]
  (.execute cleanup-executor f))
