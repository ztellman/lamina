;;   Copyright (c) Aaron VonderHaar. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.test.watch
  (:use
    [clojure test]
    [lamina.core channel]
    [lamina.core.watch]))

;;;

(deftest test-atom-sink
  (let [ch (channel 0 1 2)
        !a (atom-sink ch)]
    (is (= @!a 2))))
