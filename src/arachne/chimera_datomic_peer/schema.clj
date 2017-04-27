(ns arachne.chimera-datomic-peer.schema
  (:require [arachne.core.config.model :as m]))

(def schema
  "Schema for the module"
  (concat

    (m/type :chimera/DatomicPeerAdapter [:chimera/Adapter]
      "An adapter designed to project onto a Datomic Peer"
      (m/attr :chimera.datomic-peer-adapter/uri :one :string
        "The Datomic connection URI for this adapter")
      (m/attr :chimera.datomic-peer-adapter/wipe-on-start? :one-or-none :boolean
        "Flag to indicate whether the given database should be deleted before
         it is initialized (useful for testing)"))

    (m/type :chimera.datomic-peer.operation/Txdata []
      "Arbitrary Datomic transaction data, as a migration.

      If the txdata contains schema elements, they will not be included in the Chimera data model."

      (m/attr :chimera.datomic-peer.operation.txdata/edn :one :string
        "EDN string representation of the txdata to be added"))

    ))
