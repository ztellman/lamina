(ns lamina.wiki.channel-diagrams
  (:use
    [lamina core viz])
  (:require
    [clojure.java.io :as io])
  (:import
    [javax.imageio ImageIO]))

(def image-path "/../wikis/lamina/images/")

(def exists? (.exists (io/file image-path)))

(def padding [0.25 0.25])

(defmacro render-graph-diagram [name [ch] & body]
  `(try
     (let [~ch (channel)
           _# (do ~@body)
           ch# ~ch
           ^java.io.File file# (io/file (str image-path ~(str name) ".png"))
           image# (render-graph {:pad padding} ch#)]
       (when exists? (ImageIO/write image# "png" file#)))
     (catch Exception e#
       )))

(defmacro render-propagation-diagram [name [ch msg] & body]
  `(try
     (let [~ch (channel)
           _# (do ~@body)
           ch# ~ch
           file# (io/file (str image-path ~(str name) ".png"))
           image# (render-propagation {:pad padding} ch# ~msg)]
       (when exists? (ImageIO/write image# "png" file#)))
     (catch Exception e#
       )))

(render-graph-diagram channel-1 [ch]
  )

(render-graph-diagram channel-2 [ch]
  (enqueue ch 1))

(render-propagation-diagram channel-3 [ch 2]
  (enqueue ch 1))

(render-graph-diagram channel-4 [ch]
  (enqueue ch 1 2))

(render-graph-diagram channel-5 [ch]
  (enqueue ch 1 2)
  (read-channel ch))





