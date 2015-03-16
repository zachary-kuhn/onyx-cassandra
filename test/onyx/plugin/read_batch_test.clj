(ns onyx.plugin.read-batch-test
    (:require [clojurewerkz.cassaforte.client :as cass]
              [clojurewerkz.cassaforte.cql :as cql]
              [clojurewerkz.cassaforte.query :refer [column-definitions with]]
              [midje.sweet :refer :all]
              [onyx.plugin.cassandra]
              [onyx.queue.hornetq-utils :as hq-utils]
              [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(prn config)

(def scheduler :onyx.job-scheduler/round-robin)

(def env-config
  {:hornetq/mode :standalone
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.embedded/config ["hornetq/non-clustered-1.xml"]
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :standalone
   :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
   :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def batch-size 1000)

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config out-queue)

;; Setup the backing db (cassandra)
(def hosts ["127.0.0.1"])
(def session (cass/connect hosts))

(cql/create-keyspace session "onyx_keyspace"
                     (with {:replication
                            {:class "SimpleStrategy"
                             :replication_factor 1}}))
(cql/use-keyspace session "onyx_keyspace")

(cql/create-table
  session
  :people
  (column-definitions {:id :int
                       :name :varchar
                       :primary-key [:id]}))

(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Derek"}
   {:id 5 :name "Kristen"}])

(cql/insert-batch
  session
  :people
  people)

(def workflow [[:scan :persist]])

(def catalog
  [{:onyx/name :scan
    :onyx/ident :cassandra/read-rows
    :onyx/type :input
    :onyx/medium :cassandra
    :onyx/consumption :concurrent
    :cassandra/hosts hosts
    :cassandra/scan-options {}
    :cassandra/keyspace "onyx_keyspace"
    :cassandra/table :people
    :cassandra/partition-key :id
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size
    :onyx/doc "Output source for intermediate query results"}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
  peer-config
  {:catalog catalog :workflow workflow
   :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-utils/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(cql/drop-table session :people)
(cql/drop-keyspace session "onyx_keyspace")
(cass/disconnect session)

(fact (last results) => :done)
(fact (into #{} (map :name (butlast results)))
      => #{"Dorrene" "Mike" "Benti" "Kristen" "Derek"})