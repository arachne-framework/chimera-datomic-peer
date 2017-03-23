(ns arachne.chimera-datomic-peer
  "Utilities for working with CLJS in Arachne"
  (:require [clojure.spec :as s]
            [arachne.chimera-datomic-peer.schema :as schema]
            [arachne.chimera-datomic-peer.adapter :as peer-adapter]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.config :as cfg]
            [arachne.core.util :as u]
            [com.stuartsierra.component :as c]

            ))

(defn ^:no-doc schema
  "Return the schema for the module"
  []
  schema/schema)

(defn- configure-adapter
  "Configure a Datomic Peer adapter, adding capabilities and operations as well as the connection component"
  [cfg adapter-eid]
  (let [capability (fn [op]
                     {:chimera.adapter.capability/operation {:chimera.operation/type op}
                      :chimera.adapter.capability/atomic? true})]
    (cfg/with-provenance :module `configure-adapter
      (cfg/update cfg
        [{:db/id adapter-eid
          :chimera.adapter/capabilities (map capability [:chimera.operation/initialize-migrations
                                                         :chimera.operation/migrate
                                                         :chimera.operation/add-attribute
                                                         :chimera.operation/get
                                                         :chimera.operation/put
                                                         :chimera.operation/update
                                                         :chimera.operation/delete
                                                         :chimera.operation/delete-entity
                                                         :chimera.operation/batch])
          :chimera.adapter/dispatches peer-adapter/default-dispatches}]))))

(defn ^:no-doc configure
  "Configure the module"
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :where
                              [?a :chimera.datomic-peer-adapter/uri _]
                              [(missing? $ ?a :chimera.adapter/capabilities)]])]
    (reduce configure-adapter cfg adapters)))