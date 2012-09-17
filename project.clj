(defproject lamina "0.5.0-beta5"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.logging "0.2.4"]
                 [useful "0.8.2"]
                 [potemkin "0.1.5"]
                 [criterium "0.3.0"]
                 [com.yammer.metrics/metrics-core "2.1.0"
                  :exclusions [org.slf4j/slf4j-api]]]
  :multi-deps {:all [[com.yammer.metrics/metrics-core "2.1.0"
                      :exclusions [org.slf4j/slf4j-api]]
                     [org.clojure/tools.logging "0.2.4"]
                     [criterium "0.3.0"]
                     [useful "0.8.4"]
                     [potemkin "0.1.5"]]
               "master" [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]
               "1.2" [[org.clojure/clojure "1.2.1"]]
               "1.4" [[org.clojure/clojure "1.4.0"]]
               }
  :dev-dependencies [
                     [codox "0.6.1"]]
  :codox {:include [lamina.core
                    lamina.trace
                    lamina.viz
                    lamina.walk
                    lamina.executor
                    lamina.stats
                    lamina.api
                    lamina.time]
          :output-dir "autodoc"}
  ;;:jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-Xmx16m"]
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-Xmx2g" "-XX:NewSize=1g"]
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :test-selectors {:default #(not (some #{:wiki :benchmark :stress} (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :wiki :wiki
                   :stress #(or (:stress %) (= :stress (:tag %)))
                   :all (constantly true)}
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
