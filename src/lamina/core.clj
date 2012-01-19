;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.core
  (:use
    [potemkin])
  (:require
    [lamina.core.channel :as ch]
    [lamina.core.pipeline :as p]
    [lamina.core.probe :as pr]))

(import-fn ch/channel)
(import-macro ch/channel*)
(import-fn pr/probe-channel)
(import-fn pr/sympathetic-probe-channel)

(import-fn ch/enqueue)
(import-fn ch/receive)
(import-fn ch/read-channel)
(import-fn ch/receive-all)

(import-fn ch/siphon)
(import-fn ch/join)
(import-fn ch/fork)

(import-fn ch/close)
(import-fn ch/error)
(import-fn ch/on-closed)
(import-fn ch/on-drained)
(import-fn ch/on-error)

(import-fn ch/map*)
(import-fn ch/filter*)

(import-macro p/pipeline)
(import-macro p/run-pipeline)
(import-fn p/restart)
(import-fn p/redirect)
