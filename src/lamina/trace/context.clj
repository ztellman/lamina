;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.context
  (:use
    [potemkin])
  (:require
    [clojure.string :as str])
  (:import
    [java.io Writer]
    [java.lang.management ManagementFactory]))

(let [id (.getName (ManagementFactory/getRuntimeMXBean))
      [pid host] (str/split id #"@")
      pid (try
            (Integer/parseInt pid)
            (catch Exception e
              nil))]
  (def pid pid)
  (def host host))

(def ctx {:host host, :pid pid})

(def context-builder (atom nil))

(defn register-context-builder
  "Defines a function which is given a default context map, and returns a modified
   context map."
  [f]
  (reset! context-builder f))

(defn context []
  (if-let [f @context-builder]
    (f ctx)
    ctx))
