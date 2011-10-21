(defproject lamina "0.4.1-SNAPSHOT"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/algo.generic "0.1.0-SNAPSHOT"]
                 [org.clojure/core.incubator "0.1.0"]
                 [org.clojure/math.combinatorics "0.0.2-SNAPSHOT"]
                 [org.clojure/tools.logging "0.2.3"]
                 [potemkin "0.1.0"]]
  ;;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,address=8030,server=y,suspend=n"]	
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
