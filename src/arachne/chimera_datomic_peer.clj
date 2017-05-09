(ns arachne.chimera-datomic-peer
  "Utilities for working with CLJS in Arachne"
  (:require [clojure.spec.alpha :as s]
            [arachne.chimera-datomic-peer.schema :as schema]
            [arachne.chimera-datomic-peer.adapter :as peer-adapter]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.config :as cfg]
            [arachne.core.util :as u]
            [com.stuartsierra.component :as c]))

(defn ^:no-doc schema
  "Return the schema for the module"
  []
  schema/schema)

(defn ^:no-doc configure
  "Configure the module"
  [cfg]
  (let [adapters (cfg/q cfg '[:find [?a ...]
                              :where
                              [?a :chimera.datomic-peer-adapter/uri _]])]
    (reduce peer-adapter/configure-adapter cfg adapters)))

(defn conn
  "Retrieve a Datomic Peer connection object for the given adapter component"
  [adapter]
  (peer-adapter/conn adapter))
