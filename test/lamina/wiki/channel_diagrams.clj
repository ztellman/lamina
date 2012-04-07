(ns lamina.wiki.channel-diagrams
  (:use
    [lamina core viz])
  (:require
    [clojure.java.io :as io])
  (:import
    [javax.imageio ImageIO]))

(def image-path "/Users/zach/clj/wikis/lamina/images/")

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

(render-propagation-diagram channel-6 [ch 42]
  (map* inc ch))

(render-graph-diagram channel-7 [ch]
  (enqueue ch 1 2 3))

(render-graph-diagram channel-8 [ch]
  (enqueue ch 1 2 3)
  (map* inc ch))

(render-propagation-diagram channel-9 [ch 1]
  (map* inc ch)
  (map* dec ch))

(render-propagation-diagram channel-10 [ch 3]
  (enqueue ch 1 2)
  (map* inc ch)
  (map* dec ch))

(render-graph-diagram channel-11 [ch]
  (enqueue ch 1 2)
  (fork ch))

(render-propagation-diagram channel-12 [ch 3]
  (enqueue ch 1 2)
  (map* inc (fork ch))
  (map* dec (fork ch)))

(render-graph-diagram channel-13 [ch 3]
  (enqueue ch 1 2)
  (map* inc (fork ch))
  (map* dec (fork ch))
  (ground ch))

(render-propagation-diagram channel-14 [ch 1]
  (->> ch (map* inc) (filter even?)))

(render-graph-diagram channel-15 [ch]
  (enqueue ch 1 3 2)
  (reductions* max ch))

(render-graph-diagram channel-16 [ch]
  (enqueue ch 1 2)
  (take* 4 ch))

(render-graph-diagram channel-17 [ch]
  (enqueue ch 1 2)
  (take* 4 ch)
  (enqueue ch 3 4 5))

(render-graph-diagram channel-18 [ch]
  (enqueue ch 1 2 3 4))

(render-graph-diagram channel-19 [ch]
  (receive-all
    (->> (map* inc) (filter* even?))
    println))

(render-graph-diagram channel-20 [ch]
  (enqueue ch 1 2 3)
  (close ch))

(render-graph-diagram channel-21 [ch]
  (close ch))




