(ns lamina.core.operators
  (:use [lamina.core channel pipeline]))

(defn sample-every
  "Returns a channel which will emit the last message enqueued into 'ch' every 'period'
   milliseconds."
  [period ch]
  (let [ch* (channel)
	val (atom ::none)]
    (receive-all ch
      #(when-not (and (drained? ch) (nil? %))
	 (reset! val %)))
    (run-pipeline nil
      (wait-stage period)
      (fn [_]
	(let [val @val]
	  (when-not (= val ::none)
	    (enqueue ch* val))))
      (fn [_]
	(if-not (drained? ch)
	  (restart)
	  (close ch*))))
    ch*))
