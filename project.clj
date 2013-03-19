(defproject lamina "0.5.0-beta14"
  :description "event-driven data structures for clojure"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/tools.logging "0.2.4"]
                 [org.flatland/useful "0.9.0"]
                 [potemkin "0.2.0"]
                 [com.yammer.metrics/metrics-core "3.0.0-20130318.142009-8"
                  :exclusions [[org.slf4j/slf4j-api]
                               [com.yammer.metrics/metrics-annotation]]]]
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.4.0"]
                                  [criterium "0.3.1"]]}
             :1.2 {:dependencies [[org.clojure/clojure "1.2.1"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}}
  :aliases {"all" ["with-profile" "1.2,dev:1.3,dev:dev:1.5,dev"]}
  :plugins [[codox "0.6.2"]]
  :codox {:include [lamina.core
                    lamina.trace
                    lamina.viz
                    lamina.walk
                    lamina.executor
                    lamina.stats
                    lamina.api
                    lamina.time]
          :output-dir "autodoc"}
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-Xmx2g" "-XX:NewSize=1g" "-XX:MaxPermSize=256m"]
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
