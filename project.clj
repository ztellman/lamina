(defproject lamina "0.5.0-SNAPSHOT"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [criterium "0.2.0"]
                 [potemkin "0.1.1-SNAPSHOT"]]
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-XX:MaxInlineSize=100"]	
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark
                   :all (constantly true)}
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
