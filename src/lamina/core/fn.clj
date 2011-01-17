;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.fn
  (:use [lamina.core channel pipeline]))

(defn async
  "Given a function, returns an asynchronous variant of that function.

   An asynchronous function returns a result-channel instead of a value.  If any
   parameters that it is passed are result-channels, it will delay execution until
   all result channels have emitted a value.

   If any of the input result-channels emit errors, the function will not execute
   and simply emit the input error."
  [f]
  (fn [& args]
    (apply run-pipeline []
      (concat
	(map (fn [x] (read-merge (constantly x) conj)) args)
	[#(apply f %)]))))


