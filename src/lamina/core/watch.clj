;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.watch
  (:use
    [lamina.core channel utils])
  (:import
    [java.lang.ref
     WeakReference]
    [java.util
     Collections
     WeakHashMap]))

;;;

(deftype WeakRef [^WeakReference ref]
  clojure.lang.IDeref
  (deref [_] (.get ref)))

(defn weak-ref [x]
  (WeakRef. x))

;;;

(defprotocol Watcher
  (examine [_])
  (update [_ val]))

(defn ref-watcher [reference callback]
  (let [r (weak-ref reference)
        tl (ThreadLocal.)
        val (atom @reference)]
    (reify Watcher
      (examine [_]
        (when-let [r @r]
          (loop [x @val, y @r]
            (if-not (= x y)
              (if (compare-and-set! val x y)
                (callback y)
                (recur @val @r)))))))))

(defn watcher-channel [watchable]
  (let [ch (channel @watchable)
        k (gensym "watcher-channel")]
    (add-watch watchable k
      (fn [_ _ _ x] (enqueue ch x)))
    (on-closed ch
      #(remove-watch watchable k))
    ch))
