(ns arachne.chimera-datomic-peer.adapter.operations
  (:require [arachne.core.config :as cfg]
            [arachne.chimera.migration :as mig]
            [arachne.chimera.adapter :as a]
            [arachne.error :as e :refer [deferror error]]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]))

(s/def :chimera.datomic-peer.operation/txdata
  (s/fspec :args (s/cat :adapter :chimera/adapter
                   :op #{:chimera.datomic-peer.operation/txdata}
                   :payload (s/keys :req [:chimera.datomic-peer.operation.txdata/edn])
                   :context any?)
    :ret vector?))

(defmethod mig/operation->model :chimera.datomic-peer.operation/txdata
  [cfg adapter migration operation]
  ;; txdata can't update Chimera's model so this is a no-op
  cfg)

(deferror ::txdata-parsing-error
  :message "Error while parsing EDN data from txdata operation"
  :explanation "A migration named `:migration-name` contained an Datomic txdata operation.

  Txdata operations store txdata as an EDN string in the configuration. However, an exception was thrown while trying to parse this string back into Clojure data structures so it can be transacted."
  :suggestions ["Ensure that the txdata contains only forms that can be serialized and deserialized by EDN"]
  :ex-data-docs {:string "The string that could not be parsed"
                 :migration "The migration"
                 :migration-name "The migration's name"
                 :operation "The operation"
                 :cfg "The cfg"})

(defn get-txdata
  "Parse the txdata from an operation, throwing an exception if there is a problem."
  [cfg migration operation]
  (let [edn (:chimera.datomic-peer.operation.txdata/edn operation)]
    (try
      (edn/read-string {:data-readers *data-readers*} edn)
      (catch Exception e
        (error ::txdata-parsing-error
          {:string edn
           :migration-name (:chimera.migration/name migration)
           :migration migration
           :operation operation
           :cfg cfg}
          e)))))

(defmethod mig/canonical-operation :chimera.datomic-peer.operation/txdata
  [cfg migration operation]
  (assoc operation :chimera.datomic-peer.operation.txdata/edn
                   (get-txdata cfg migration operation)))
