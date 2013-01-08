;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.trace.router
  (:use
    [clojure.walk :only (postwalk)])
  (:require
    [lamina.trace.router
     [core :as c]
     [parse :as p]
     [operators :as o]]))

(defn- stringify-keys [x]
  (postwalk
    (fn [x]
      (if (map? x)
        (zipmap
          (map #(if (keyword? %) (name %) (str %)) (keys x))
          (vals x))
        x))
    x))

(defn ? [transform-descriptor ch]
  (let [transform-descriptor (if (string? transform-descriptor)
                               (p/parse-stream (str "." transform-descriptor))
                               transform-descriptor)]
    (c/transform-trace-stream (stringify-keys transform-descriptor) ch)))
