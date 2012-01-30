;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.utils
  (:require
    [clojure.tools.logging :as log]))

(defprotocol IEnqueue
  (enqueue [_ msg]))

(defn in-transaction? []
  (clojure.lang.LockingTransaction/isRunning))

(defmacro defer-within-transaction [[defer-fn default-response override?] & body]
  `(if ~(if override?
          `(and (not ~override?) (in-transaction?))
          `(in-transaction?))
     (do
       (send (agent nil) (fn [_#]
                           (try
                             ~defer-fn
                             (catch Exception e#
                               (log/error e# "Error in deferred action.")))))
       ~default-response)
     (do ~@body)))
