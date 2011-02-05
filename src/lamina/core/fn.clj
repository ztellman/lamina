;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core.fn
  (:use
    [lamina.core channel pipeline]
    [clojure walk]))

(def special-form?
  (set '(let if do let fn quote var def)))

(defmacro async
  "Performs magic.

   Any expression in a block wrapped by (async ...) can use a result-channel instead of an
   actual value, and defer its own execution (and the execution of all code that depends on
   its value) until the result-channel has emitted a value.  The value returned from an
   (async ...) block is always a result-channel.

   This means that we can write code that looks like a normal function, but is actually several
   distinct callbacks stitched together.  Consider a situation where we want to read two messages
   from a channel.  We could compose nested callbacks:

   (receive ch
     (fn [first-message]
       (receive ch
         (fn [second-message]
           (perform-action [first-message second-message])))))

   However, using async and read-channel (which returns a result-channel representing the next
   message in the channel), this becomes a lot more straightforward:

   (async
     (let [first-message (read-channel ch)
           second-message (read-channel ch)]
       [first-message second-message]))

   This will return a result-channel which will emit a vector of the next two messages from the
   channel, once they've both arrived.

   This is very, very experimental, and may be subject to change."
  [& body]
  `(do
     ~@(postwalk
	 (fn [expr]
	   (if-not (and
		     (list? expr)
		     (< 1 (count expr))
		     (not (special-form? (first expr))))
	     expr
	     `(run-pipeline []
		~@(map
		    (fn [arg] `(read-merge (constantly ~arg) conj))
		    (rest expr))
		(fn [args#]
		  (apply ~(first expr) args#)))))
	 body)))

(defmacro pfn
  "A variant of fn that optionally accepts result-channels instead of parameters, and
   returns a result-channel representing the returned value.

   The function will always immediately return a result-channel, but the result-channel
   will only emit a value once all input result-channels have emitted their value.  If any of
   the input result-channels emit errors, the function will not execute and simply emit the
   input error."
  [& args]
  `(let [f# (fn ~@args)]
     (fn ~@(when (symbol? (first args)) (take 1 args))
       [~'& args#]
       (apply run-pipeline []
	 (concat
	   (map (fn [x#] (read-merge (constantly x#) conj)) args#)
	   [#(apply f# %)])))))


