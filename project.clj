(defproject lamina "0.5.0-beta15"
  :description "event-driven data structures for clojure"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/tools.logging "0.2.4"]
                 [org.flatland/useful "0.9.0"]
                 [potemkin "0.2.1"]]
  :exclusions [org.clojure/clojure
               org.clojure/contrib
               org.clojure/clojure-contrib]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]
                                  [criterium "0.3.1"]
                                  [codox-md "0.2.0"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}}
  :aliases {"all" ["with-profile" "1.6,dev:1.3,dev:dev:1.4,dev"]}
  :plugins [[codox "0.6.2"]]
  :codox {:writer codox-md.writer/write-docs
          :include [lamina.core
                    lamina.trace
                    lamina.viz
                    lamina.walk
                    lamina.executor
                    lamina.stats
                    lamina.api
                    lamina.query
                    lamina.time]
          :output-dir "autodoc"}
  :jvm-opts ["-server"
             "-XX:+UseConcMarkSweepGC"
             "-Xmx2g"
             "-XX:NewSize=1g"
             "-XX:MaxPermSize=256m"]
  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"}
  :test-selectors {:default #(not (some #{:wiki :benchmark :stress}
                                        (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :wiki :wiki
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)}
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :warn-on-reflection true)
