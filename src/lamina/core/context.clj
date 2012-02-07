;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.context
  (:use
    [useful.datatypes :only (assoc-record make-record)])
  (:import
    [java.util
     LinkedList]))

(set! *warn-on-reflection* true)

;;;

(deftype Context
  [timer])

;;;

(def ^ThreadLocal stacks (ThreadLocal.))

(defn ^Context context []
  (when-let [^LinkedList stack (.get stacks)]
    (.peek stack)))

(defmacro push-context [& args]
  `(if-let [^LinkedList stack# (.get stacks)]
     (if-let [^Context context# (.peek stack#)]
       (.addFirst stack# (assoc-record context# ~@args))
       (.addFirst stack# (make-record Context ~@args)))
     (let [^LinkedList stack# (LinkedList.)]
       (.addFirst stack# (make-record Context ~@args))
       (.set stacks stack#))))

(defn pop-context []
  (.removeFirst ^LinkedList (.get stacks)))

(defmacro with-context [context & body]
  `(let [^LinkedList stack# (if-let [stack# (.get stacks)]
                              stack#
                              (let [stack# (LinkedList.)]
                                (.set stacks stack#)
                                stack#))
         context# ~context]
     (.addFirst stack# context#)
     (try
       ~@body
       (finally
         (when (identical? context# (.peek stack#))
           (.removeFirst stack#))))))

;;;

(defn timer []
  (when-let [^Context ctx (context)]
    (.timer ctx)))
