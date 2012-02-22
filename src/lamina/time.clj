;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.time)

(defn nanoseconds [n]
  (/ n 1e6))

(defn microseconds [n]
  (/ n 1e3))

(defn milliseconds [n]
  n)

(defn seconds [n]
  (* n 1e3))

(defn minutes [n]
  (* n 60e3))

(defn hours [n]
  (* n 36e4))

(defn hz [n]
  (/ 1e3 n))
