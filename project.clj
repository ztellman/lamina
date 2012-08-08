(defproject lamina "0.4.1"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [potemkin "0.1.3"]]
  :multi-deps {:all [[org.clojure/tools.logging "0.2.3"]
                     [potemkin "0.1.3"]]
               "1.2" [[org.clojure/clojure "1.2.1"]]}
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib]
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
