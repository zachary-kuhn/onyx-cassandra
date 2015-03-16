(ns onyx.plugin.write-batch-test
  (:require [onyx.plugin.cassandra]
            [clojurewerkz.cassaforte.client :as cass]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :refer [column-definitions with]]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-utils]
            [onyx.api]))

;; Setup the environment
(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

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

;; Setup our test input stream (we should probably be using async here..)
(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def in-queue (str (java.util.UUID/randomUUID)))

(hq-utils/create-queue! hq-config in-queue)

;; Define our data..
(def people
  [{:id 1 :name "Mike"}
   {:id 2 :name "Dorrene"}
   {:id 3 :name "Benti"}
   {:id 4 :name "Kristen"}
   {:id 5 :name "Derek"}])

;; And push it into the queue..
(hq-utils/write-and-cap! hq-config in-queue people 1)


;; Now lets define the workflow and catalog..
;; Bit of a weird ordering here since you'd usually setup your system first, but it does illustrate
;; the flexibility of Onyx
(def workflow {:in {:identity :out}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 2}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 2}

   {:onyx/name :out
    :onyx/ident :cassandra/write-rows
    :onyx/type :output
    :onyx/medium :cassandra
    :onyx/consumption :concurrent
    :cassandra/hosts hosts
    :cassandra/keyspace "onyx_keyspace"
    :cassandra/table :people
    :cassandra/partition-key :id
    :onyx/batch-size 2
    :onyx/doc "Transacts segments to cassandra"}])

;; Spin up a single peer and throw the job at it.
(def v-peers (onyx.api/start-peers! 1 peer-config))

(def job-id (onyx.api/submit-job
              peer-config
              {:catalog        catalog :workflow workflow
               :task-scheduler :onyx.task-scheduler/round-robin}))

(onyx.api/await-job-completion peer-config job-id)

;; Get the results
(def results (cql/select session :people))

;; Clean up
(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(cql/drop-table session :people)
(cql/drop-keyspace session "onyx_keyspace")
(cass/disconnect session)

;; Assert that what we expected to happen, did.
(fact (set (map :name results)) => (set (map :name people)))