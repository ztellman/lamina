;;   Copyright (c) Zachary Tellman. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns lamina.viz.pipeline
  (:use [lamina.viz core]))

(def pipeline-frame (gen-frame "pipeline"))

(defn stage->nodes [stage]
  [[[stage 0] {:label (name stage)}]
   [[stage 1] {:label "hello"}]])

(defn simple-pipeline [stages]
  (let [stages->nodes (zipmap stages (map stage->nodes stages))
        subgraphs (map #(vector :subgraph %) stages)
        node->subgraph (zipmap stages subgraphs)]
    {:options {:compound true}
     :nodes (->> stages->nodes vals (apply concat) (into {}))
     :hierarchy (map vector subgraphs)
     :subgraph->nodes (zipmap subgraphs (map #(->> % stages->nodes (map first)) stages))
     :edges (concat
              (->> (partition 2 1 stages)
                (map
                  (fn [[a b]]
                    {:src (->> a stage->nodes last first)
                     :dst (->> b stage->nodes ffirst)
                     :lhead (node->subgraph b)
                     :ltail (node->subgraph a)})))
              (->> stages
                (map stage->nodes)
                (map #(map first %))
                (map #(partition 2 1 %))
                (apply concat)
                (map (fn [[a b]] {:src a, :dst b}))))}))

(defn view-stages [stages]
  (->> stages simple-pipeline digraph println)
  (->> stages simple-pipeline digraph (view-dot-string pipeline-frame)))
