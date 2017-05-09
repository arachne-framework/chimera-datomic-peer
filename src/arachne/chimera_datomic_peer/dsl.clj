(ns arachne.chimera-datomic-peer.dsl
  "DSL code to to create a Datomic Peer Chimera Adapter"
  (:require [clojure.spec.alpha :as s]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.dsl :as core]
            [arachne.core.config :as cfg]
            [arachne.core.config.script :as script :refer [defdsl]]
            [arachne.core.util :as u]
            [arachne.chimera.specs]))

(s/def ::ensure boolean?)
(s/def ::wipe boolean?)

(defdsl adapter
  "Define a Datomic Peer adapter, using the specified Datomic URI and migrations.

  Takes the following options:

  :ensure - Invoke `apply-migrations` when the adapter starts.
  ;wipe - Erase the database when the adapter starts. Use only for testing!"
  (s/cat :uri string?
         :migrations (s/coll-of :chimera.migration/name :min-count 1)
         :options (u/keys** :opt-un [::ensure ::wipe]))
  [uri migrations & options]
  (let [tid (cfg/tempid)]
    (script/transact
      [(u/mkeep
         {:db/id tid
          :chimera.datomic-peer-adapter/uri (:uri &args)
          :chimera.adapter/apply-migrations-on-start? (-> &args :options second :ensure)
          :chimera.datomic-peer-adapter/wipe-on-start? (-> &args :options second :wipe)
          :chimera.adapter/migrations (map (fn [mig-name]
                                             {:chimera.migration/name mig-name})
                                        (:migrations &args))})]
      tid)))

(defdsl seed
  "Define a migration operation that transacts the specified txdata into the
   database. Any schema elements present in the txdata will *not* be reflected
   in Chimera's data model."
  (s/cat :txdata vector?)
  [txdata]
  {:db/id (cfg/tempid)
   :chimera.migration.operation/type :chimera.datomic-peer.operation/txdata
   :chimera.datomic-peer.operation.txdata/edn (pr-str txdata)})
