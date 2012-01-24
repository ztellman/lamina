(defproject lamina "0.5.0-SNAPSHOT"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [useful "0.7.3"]
                 [potemkin "0.1.1"]]
  :multi-deps {:all [[org.clojure/tools.logging "0.2.3"]
                     [useful "0.7.3"]
                     [potemkin "0.1.1"]]
               "1.2" [[org.clojure/clojure "1.2.1"]]
               "1.4a" [[org.clojure/clojure "1.4.0-alpha4"]]
               "1.4s" [[org.clojure/clojure "1.4.0-master-SNAPSHOT"]]
               }
  :dev-dependencies [[criterium "0.2.1-SNAPSHOT"]]
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-Xmx1g" "-XX:NewSize=256m"]
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :test-selectors {:default #(not (some #{:benchmark :stress} (keys %)))
                   :benchmark :benchmark
                   :stress :stress
                   :all (constantly true)}
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
