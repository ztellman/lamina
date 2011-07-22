;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.api
  (:use
    [potemkin])
  (:require
    [lamina.core.pipeline :as pipeline]
    [lamina.core.channel :as channel]
    [lamina.core.seq :as seq]))

(import-fn #'pipeline/poll-result)
(import-fn #'pipeline/success-result)
(import-fn #'pipeline/error-result)
(import-fn #'pipeline/success!)
(import-fn #'pipeline/error!)
(import-fn #'pipeline/closed-result)
(import-fn #'pipeline/drained-result)

(import-fn #'seq/copy)

(import-fn #'channel/dequeue)
