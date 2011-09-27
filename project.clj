(defproject lamina "0.4.0-beta2-SNAPSHOT"
  :description "event-driven data structures for clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.1.2"]
                 [org.clojure.contrib/core "1.3.0-alpha4"]
                 [org.clojure.contrib/generic "1.3.0-alpha4"]
                 [org.clojure.contrib/combinatorics "1.3.0-alpha4"]
                 [potemkin "0.1.0"]]
  :exclusions [org.clojure/contrib
               org.clojure/clojure-contrib
               org.slf4j/slf4j-log4j12
               org.slf4j/slf4j-jdk14]
  ;;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,address=8030,server=y,suspend=n"]	
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo})
