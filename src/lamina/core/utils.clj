(ns lamina.core.utils)

(def original-fn ::original)

(defn unwrap-fn [f]
  (if-let [unwrapped (-> f meta original-fn)]
    unwrapped
    f))


