;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.executors
  (:use
    [potemkin]
    [lamina trace])
  (:require
    [lamina.executors.core :as x]))

(import-fn #'x/set-default-executor)
(import-fn #'x/set-local-executor)

(import-fn #'x/thread-pool)
(import-fn #'x/thread-pool?)
(import-fn #'x/shutdown-thread-pool)

(defmacro with-thread-pool
  "Executes the body on the specified thread pool.  Returns a result-channel representing the
   eventual return value.

   The thread pool may be optionally followed by a map specifying options that override those
   given when the thread pool was created.  If, for instance, we want to have the timeout be
   100ms in this particular instance, we can use:

   (with-thread-pool pool {:timeout 100}
     ...)"
  [pool options & body]
  (apply x/expand-with-thread-pool pool options body))

(defn executor
  "Given a thread pool and a function, returns a function that will execute that function
   on a thread pool and returns a result-channel representing its eventual value.

   The returned function takes a sequence of arguments as its first argument, and thread
   pool configurations as an optional second argument.

   > (def f +)
   #'f
   > (f 1 2)
   3
   > (def f* (executor (thread-pool) f))
   #'f*
   > @(f* [1 2] {:timeout 100})
   3"
  ([pool f]
     (executor pool f nil))
  ([pool f options]
     (trace-wrap
       (fn this
	 ([args]
	    (this args nil))
	 ([args inner-options]
	    (let [options (merge options inner-options)
		  timeout (:timeout options)
		  timeout (cond
			    (nil? timeout) -1
			    (fn? timeout) (apply timeout args)
			    :else timeout)]
	      (with-thread-pool pool (assoc options
				       :args args
				       :timeout timeout)
		(apply f args)))))
       (merge
	 {:name (gensym "executor.")}
	 options
	 {:args-transform first}))))
