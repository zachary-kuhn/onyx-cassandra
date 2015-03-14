(ns onyx.plugin.cassandra
    (:require [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [clojurewerkz.cassaforte.client :as cass]
              [clojurewerkz.cassaforte.cql :as cql]))

(defmethod l-ext/inject-lifecycle-resources
  :cassandra/scan
  [_ {:keys [onyx.core/task-map]  :as pipeline}]
  {:cassandra/last-prim-kvs (atom false)})

(defmethod p-ext/read-batch [:input :cassandra-scan]
  [{:keys [onyx.core/task-map onyx.core/task :cassandra/last-prim-kvs] :as pipeline}]

  (cond
    (nil? @last-prim-kvs)
    (do
      {:onyx.core/batch [{:input task :message :done}]})
    :else
    (let [opts (:cassandra/scan-options task-map)
          last @last-prim-kvs
          opts (if last (assoc opts :last-prim-kvs last) opts)
          result
          (cql/select
            (cass/connect (:cassandra/host task-map))
            (:cassandra/table task-map)
            (assoc opts :limit (:onyx/batch-size task-map)))
          last-kv (-> result meta :last-prim-kvs)]
      (reset! last-prim-kvs last-kv)
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
      (cass/connect (:cassandra/host task-map))
      (:cassandra/table task-map)
      compressed))
  {:onyx.core/written? true})