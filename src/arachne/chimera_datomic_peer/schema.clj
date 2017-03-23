(ns arachne.chimera-datomic-peer.schema
  (:require [arachne.core.config.model :as m]))

(def schema
  "Schema for the module"
  (concat

    (m/type :chimera/DatomicPeerAdapter [:chimera/Adapter]
      "An adapter designed to project onto a Datomic Peer"
      (m/attr :chimera.datomic-peer-adapter/uri :one :string
        "The Datomic connection URI for this adapter"))))
