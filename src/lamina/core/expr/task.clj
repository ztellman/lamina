(ns lamina.core.expr.task
  (:use lamina.core.pipeline))

(def *current-executor* nil)
(def default-executor (atom nil))
(def ns-executors (atom {}))

(defn set-default-executor
  "Sets the default executor used by task."
  [executor]
  (reset! default-executor executor))

(defn set-local-executor
  "Sets the executor used by task when called within the current namespace."
  [executor]
  (swap! ns-executors assoc *ns* executor))

(defmacro current-executor []
  (let [ns *ns*]
    `(or
       *current-executor*
       (@ns-executors ~ns)
       @default-executor
       clojure.lang.Agent/soloExecutor)))

(defn transform-task [body]
  `(let [result# (result-channel)
	 executor# (current-executor)]
     (.submit executor#
       (fn []
	 (binding [*current-executor* executor#]
	   (siphon-result
	     (run-pipeline nil
	       :error-handler (constantly nil)
	       (fn [_#]
		 ~@body))
	     result#))))
     result#))
