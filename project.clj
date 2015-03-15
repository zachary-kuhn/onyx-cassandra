(defproject onyx-cassandra "0.5.3"
  :description "Onyx plugin for cassandra"
  :url "FIX ME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.mdrogalis/onyx "0.5.3"]
                 [clojurewerkz/cassaforte "2.0.1"]
                 [org.slf4j/slf4j-simple "1.7.10"]
                 [com.taoensso/timbre "3.4.0"]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [org.hornetq/hornetq-core-client "2.4.0.Final"]]
                   :plugins [[lein-midje "3.1.3"]]}})
