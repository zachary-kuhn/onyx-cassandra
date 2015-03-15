(ns onyx.plugin.cassandra
    (:require [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [clojurewerkz.cassaforte.client :as cass]
              [clojurewerkz.cassaforte.cql :as cql]
              [clojurewerkz.cassaforte.query :refer [paginate token]]))

(defmethod l-ext/inject-lifecycle-resources
  :cassandra/scan
  [_ {:keys [onyx.core/task-map]  :as pipeline}]
  {:cassandra/last-key (atom false)
   :cassandra/session (cass/connect (:cassandra/hosts task-map) (:cassandra/keyspace task-map))})

(defmethod p-ext/read-batch [:input :cassandra-scan]
  [{:keys [onyx.core/task-map onyx.core/task cassandra/last-key cassandra/session] :as pipeline}]

  (cond
    (nil? @last-key)
    {:onyx.core/batch [{:input task :message :done}]}
    
    :else
    (let [opts (assoc (:cassandra/scan-options task-map) :limit (:onyx/batch-size task-map))
          lk @last-key
          opts (if lk (assoc opts :where [[> (token (:cassandra/partition-key task-map)) (token lk)]]) opts)
          result
          (cql/select
            session
            (:cassandra/table task-map)
            opts)
          last-key-in-set (if result ((:cassandra/partition-key task-map) (last result)))]
      (reset! last-key last-key-in-set)
      {:onyx.core/batch
       (map (fn [r] {:input task :message r}) result)})))

(defmethod p-ext/decompress-batch [:input :cassandra-scan]
  [{:keys [onyx.core/batch] :as event}]
  {:onyx.core/decompressed (filter identity (map :message batch))})

(defmethod p-ext/strip-sentinel [:input :cassandra-scan]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/tail-batch? (= (last decompressed) :done)
   :onyx.core/requeue? false
   :onyx.core/decompressed (remove (partial = :done) decompressed)})

(defmethod p-ext/apply-fn [:input :cassandra-scan]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/results decompressed})

(defmethod p-ext/apply-fn [:output :cassandra]
  [_] {})

(defmethod p-ext/compress-batch [:output :cassandra]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  {:onyx.core/compressed decompressed})

(defmethod p-ext/write-batch [:output :cassandra]
  [{:keys [onyx.core/compressed onyx.core/task-map] :as pipeline}]
  (when-not (empty? compressed)
    (cql/insert-batch
      (cass/connect (:cassandra/hosts task-map))
      (:cassandra/table task-map)
      compressed))
  {:onyx.core/written? true})