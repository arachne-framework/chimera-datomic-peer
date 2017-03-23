(ns arachne.chimera-datomic-peer.adapter
  "Chimera adapter implementation"
  (:require [clojure.spec :as s]
            [datomic.api :as d]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.dsl :as core]
            [arachne.core.config :as cfg]
            [arachne.core.config.script :as script :refer [defdsl]]
            [arachne.core.util :as u]
            [arachne.chimera.adapter :as adapter]
            [com.stuartsierra.component :as c]))

(def default-dispatches
  "Baseline dispatches for all Datomic Peer adapters"
  [{:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/initialize-migrations}
    :chimera.adapter.dispatch/impl ::init-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/migrate}
    :chimera.adapter.dispatch/impl ::migrate-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/put}
    :chimera.adapter.dispatch/impl ::put-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/get}
    :chimera.adapter.dispatch/impl ::get-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/update}
    :chimera.adapter.dispatch/impl ::update-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete-entity}
    :chimera.adapter.dispatch/impl ::delete-entity-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "[_ _ _]"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete}
    :chimera.adapter.dispatch/impl ::delete-attr-value-op}
   {:chimera.adapter.dispatch/index 1,
    :chimera.adapter.dispatch/pattern "[_ _]"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/delete}
    :chimera.adapter.dispatch/impl ::delete-attr-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/batch}
    :chimera.adapter.dispatch/impl ::batch-op}])

(defn init-op
  "Implementation for :chimera.operation/initialize-migrations.

  In Datomic's case, this involves creating the database and installing the schema required to track migrations."
  [adapter _ _]
  (throw (ex-info "not yet implemented" {})))

(defn migrate-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn put-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn get-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn update-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn delete-entity-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn delete-attr-value-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn delete-attr-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))

(defn batch-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op]
  (throw (ex-info "not yet implemented" {})))