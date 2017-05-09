(ns arachne.chimera-datomic-peer.adapter
  "Chimera adapter implementation"
  (:require [clojure.spec.alpha :as s]
            [datomic.api :as d]
            [arachne.error :as e :refer [deferror error]]
            [arachne.core.dsl :as core]
            [arachne.core.config :as cfg]
            [arachne.core.config.script :as script :refer [defdsl]]
            [arachne.core.util :as u]
            [arachne.chimera.adapter :as adapter]
            [arachne.chimera.migration :as mig]
            [arachne.chimera :as ch]
            [arachne.chimera.operation :as cho]
            [arachne.chimera-datomic-peer.adapter.pull-expressions :as pe]
            [arachne.chimera-datomic-peer.adapter.operations :as ops]
            [com.stuartsierra.component :as c]
            [clojure.walk :as w]))

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
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.operation/add-attribute}
    :chimera.adapter.dispatch/impl ::add-attr-op}
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
    :chimera.adapter.dispatch/impl ::batch-op}
   {:chimera.adapter.dispatch/index 0,
    :chimera.adapter.dispatch/pattern "_"
    :chimera.adapter.dispatch/operation {:chimera.operation/type :chimera.datomic-peer.operation/txdata}
    :chimera.adapter.dispatch/impl ::txdata-op}])

(defn configure-adapter
  "Configure a Datomic Peer adapter, adding all the attributes a Datomic Peer needs"
  [cfg adapter-eid]
  (let [capability (fn [op]
                     {:chimera.adapter.capability/operation {:chimera.operation/type op}
                      :chimera.adapter.capability/atomic? true})]
    (cfg/with-provenance :module `configure-adapter
      (cfg/update cfg
        [{:db/id (cfg/tempid)
          :chimera.operation/type :chimera.datomic-peer.operation/txdata
          :chimera.operation/idempotent? false
          :chimera.operation/batchable? true}
         {:db/id adapter-eid
          :chimera.adapter/capabilities (map capability [:chimera.operation/initialize-migrations
                                                         :chimera.operation/migrate
                                                         :chimera.operation/add-attribute
                                                         :chimera.operation/get
                                                         :chimera.operation/put
                                                         :chimera.operation/delete
                                                         :chimera.operation/delete-entity
                                                         :chimera.operation/batch
                                                         :chimera.datomic-peer.operation/txdata])
          :chimera.adapter/dispatches default-dispatches
          :chimera.adapter/start ::start-adapter}]))))

(defn start-adapter
  "Function called when the adapter starts, returning an updated adapter"
  [adapter]
  (when (:chimera.datomic-peer-adapter/wipe-on-start? adapter)
    (d/delete-database (:chimera.datomic-peer-adapter/uri adapter)))
  (assoc adapter ::pull-exprs (pe/build-pull-expressions adapter)))

(defn conn
  "Return the Datomic connection object for the given Datomic adapter"
  [adapter]
  (let [uri (:chimera.datomic-peer-adapter/uri adapter)]
    (d/connect uri)))

(defn init-op
  "Implementation for :chimera.operation/initialize-migrations.

  In Datomic's case, this involves creating the database and installing the schema required to track migrations."
  [adapter _ _]
  (let [uri (:chimera.datomic-peer-adapter/uri adapter)
        created? (d/create-database uri)]
    (when created?
      @(d/transact (d/connect uri)
         [{:db/id (d/tempid :db.part/db)
           :db/ident :chimera.migration/name
           :db/unique :db.unique/value
           :db/valueType :db.type/keyword
           :db/cardinality :db.cardinality/one
           :db/doc "The unique name for the migration"}
          {:db/id (d/tempid :db.part/db)
           :db/ident :chimera.migration/signature
           :db/valueType :db.type/string
           :db/cardinality :db.cardinality/one
           :db/doc "The MD5 signature of the migration"}
          {:db/id "chimera-partition"
           :db/ident :db.part/chimera}
          [:db/add :db.part/db :db.install/partition "chimera-partition"]]))
    true))

(defn- datomic-cardinality
  "Given a Chimera attribute, return the Datomic cardinality"
  [attr]
  (if (= 1 (:chimera.attribute/max-cardinality attr))
    :db.cardinality/one
    :db.cardinality/many))

(defn- datomic-valuetype
  "Given a Chimera attribute, return the Datomic valuetype"
  [attr]
  (case (:chimera.attribute/range attr)
    :chimera.primitive/boolean :db.type/boolean
    :chimera.primitive/string :db.type/string
    :chimera.primitive/keyword :db.type/keyword
    :chimera.primitive/long :db.type/long
    :chimera.primitive/double :db.type/double
    :chimera.primitive/bigdec :db.type/bigdec
    :chimera.primitive/bigint :db.type/bigint
    :chimera.primitive/instant :db.type/instant
    :chimera.primitive/uuid :db.type/uuid
    :chimera.primitive/bytes :db.type/bytes
    :db.type/ref))

;; Datomic transaction context, contains txdata and a
;; map of lookups -> tempids (to track entities created in the current batch/context)
(defrecord Context [txdata entities])
(defn fresh-context [] (->Context [] {}))

(defn- unwrap-exception
  "Successively return an exception's cause until it finds an info-bearing
   exception"
  [e]
  (cond
    (ex-data e) e
    (.getCause e) (unwrap-exception (.getCause e))
    :else nil))

(defn- parse-lookup-error
  "Parse the error message of a 'not-an-entity' error, returning the missing identifier"
  [msg]
  (second (re-find #"Unable to resolve entity: (.*) in datom" msg)))

(defn transact
  "Given a Datomic adapter an operation type and a transaction context, commit
   the txdata in the transaction context"
  [adapter op context]
  (try
    @(d/transact (conn adapter) (:txdata context))
    (catch Exception e
      (let [cause (unwrap-exception e)]
        (cond
          (= :db.error/not-an-entity (:db/error (ex-data cause)))
          (error ::cho/entity-does-not-exist
            {:lookup (parse-lookup-error (.getMessage cause))
             :op op
             :adapter-eid (:db/id adapter)
             :adapter-aid (:arachne/id adapter)} e)
          :else (throw e)))))
  nil)

(defn- lu->lu-ref
  "Convenience function to convert an Arachne Lookup to a Datomic lookup ref"
  [lookup]
  [(:attribute lookup) (:value lookup)])

(defn add-attr-op
  "Implementation for :chimera.operation/add-attr"
  [adapter _ payload context]
  (let [attr (:chimera.operation.add-attribute/attr payload)
        txdata {:db/id (d/tempid :db.part/db)
                :db/ident (:chimera.attribute/name attr)
                :db/valueType (datomic-valuetype attr)
                :db/cardinality (datomic-cardinality attr)}
        txdata (if (:chimera.attribute/component attr)
                 (assoc txdata :db/isComponent true)
                 txdata)
        txdata (if (:chimera.attribute/key attr)
                 (assoc txdata :db/unique :db.unique/identity)
                 txdata)
        txdata (if (:chimera.attribute/indexed attr)
                 (assoc txdata :db/index true)
                 txdata)]
    (update context :txdata conj txdata)))

(defn- apply-migration
  "Actually apply a migration operation.

   Throw an exception if a migration with the given name already exists in the target DB"
  [adapter name signature operations]
  (let [tid (d/tempid :db.part/chimera)
        context (reduce (fn [context op]
                          (ch/operate adapter (:chimera.migration.operation/type op) op context))
                  (->Context [{:db/id tid
                               :chimera.migration/name name
                               :chimera.migration/signature signature}] nil)
                  operations)]
    (transact adapter :chimera.operation/migrate context)))

(defn migrate-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op {:keys [name signature operations] :as payload}]
  (let [c (conn adapter)
        [existing-sig t] (d/q '[:find [?sig ?inst]
                                :in $ ?name
                                :where
                                [?mig :chimera.migration/name ?name]
                                [?mig :chimera.migration/signature ?sig ?tx]
                                [?tx :db/txInstant ?inst]]
                           (d/db c)
                           name)]
    (if existing-sig
      (if (= existing-sig signature)
        nil                                                 ;; successful no-op
        (error ::mig/invalid-signature {:name name
                                        :adapter-eid (:db/id adapter)
                                        :adapter-aid (:arachne/id adapter)
                                        :original-time t
                                        :original-time-str (e/format-date t)
                                        :original existing-sig
                                        :new signature}))
      (try
        (apply-migration adapter name signature operations)
        (catch Exception e
          (if (::retried? payload)
            (throw e)
            (migrate-op adapter op (assoc payload ::retried? true))))))))

(defn- validate-cardinality
  "Given an attribute and a value, validate that it is the correct cardinality"
  [adapter op attr value]
  (let [card-many? (adapter/cardinality-many? adapter attr)]
    (when (and card-many? (not (set? value)))
      (error ::cho/unexpected-cardinality-one
        {:attribute attr
         :value value
         :op op
         :adapter-eid (:db/id adapter)
         :adapter-aid (:arachne/id adapter)}))
    (when (and (set? value) (not card-many?))
      (error ::cho/unexpected-cardinality-many
        {:attribute attr
         :value value
         :op op
         :adapter-eid (:db/id adapter)
         :adapter-aid (:arachne/id adapter)}))))

(defn put-op
  "Implementation for :chimera.operation/put

  Apply a put operation"
  ([adapter op entity-map]
   (transact adapter op (put-op adapter op entity-map (fresh-context))))
  ([adapter op entity-map {:keys [txdata entities] :as context}]
   (let [lu (ch/entity-lookup adapter entity-map)
         tid (d/tempid :db.part/user)
         convert (fn [val]
                   (if (ch/lookup? val)
                     (or (entities val) (lu->lu-ref val))
                     val))
         em (into {} (map (fn [[attr value]]
                            (validate-cardinality adapter op attr value)
                            [attr (if (set? value)
                                    (map convert value)
                                    (convert value))]) entity-map))
         em (assoc em :db/id tid)]
     (-> context
       (update :entities assoc lu tid)
       (update :txdata conj em)))))

(defn- replace-lookup-refs
  "Given a map entry, replace the values with lookup refs (if appropriate)"
  [adapter [attr value :as entry]]
  (if (and (adapter/ref? adapter attr) (not (adapter/component? adapter attr)))
    (if (adapter/cardinality-many? adapter attr)
      [attr (->> value (map first) (map ch/lookup) (set))]
      [attr (ch/lookup (first value))])
    entry))

(defn- datomic->chimera
  "Given the result of a Datomic pull, return a Chimera entity map"
  [adapter m]
  (w/prewalk (fn [node]
               (cond
                 (map-entry? node) (replace-lookup-refs adapter node)
                 (vector? node) (set node)
                 :else node))
    m))

(defn get-op
  "Implementation for :chimera.operation/get

  Apply a migration to Datomic"
  [adapter op lookup]
  (let [db (d/db (conn adapter))
        pull-expr (get-in adapter [::pull-exprs (:attribute lookup)])
        em (d/pull db pull-expr (lu->lu-ref lookup))]
    (datomic->chimera adapter em)))

(defn delete-entity-op
  "Implementation for :chimera.operation/delete-entity"
  ([adapter op lookup]
   (transact adapter op (delete-entity-op adapter op lookup (fresh-context))))
  ([adapter op lookup context]
   (update context :txdata conj
     [:db.fn/retractEntity (lu->lu-ref lookup)])))

(defn- protect-key-attr
  "Throw an exception if attr is a key attribute, since key attributes cannot be deleted"
  [adapter op lookup attr]
  (when (adapter/key? adapter attr)
    (error ::cho/cannot-delete-key-attr {:lookup lookup
                                         :attribute attr
                                         :adapter-eid (:db/id adapter)
                                         :adapter-aid (:arachne/id adapter)})))

(defn delete-attr-value-op
  "Implementation for :chimera.operation/delete"
  ([adapter op payload]
   (transact adapter op (delete-attr-value-op adapter op payload (fresh-context))))
  ([adapter op [lookup attr val] context]
   (protect-key-attr adapter op lookup attr)
   (let [val-txdata (if (ch/lookup? val)
                      (lu->lu-ref val)
                      val)
         txdata (if (adapter/component? adapter attr)
                  [:db.fn/retractEntity val-txdata]
                  [:db/retract (lu->lu-ref lookup) attr val-txdata])]
     (update context :txdata conj txdata))))

(defn delete-attr-op
  "Implementation for :chimera.operation/delete with no attribute specified.

  This is not wholly atomic, since the read of what attributes there are is
  not transactional with respect to the removal of those attributes. However,
  this is unlikely to matter, since in the case where an attribute write is
  'interlaved' between the read and the retraction, the result is the same as
  if the write had just been recieved a bit later, after the retraction."
  ([adapter op payload]
   (transact adapter op (delete-attr-op adapter op payload (fresh-context))))
  ([adapter op [lookup attr] context]
   (protect-key-attr adapter op lookup attr)
   (let [db (d/db (conn adapter))
         vals (map #(nth % 2) (d/datoms db :eavt (lu->lu-ref lookup) attr))]
     (reduce (fn [context val]
               (delete-attr-value-op adapter op [lookup attr val] context))
       context
       vals))))

(defn batch-op
  "Implementation for :chimera.operation/migrate

  Apply a migration to Datomic"
  [adapter op operations]
  (transact adapter op
    (reduce (fn [context [op-type payload]]
              (ch/operate adapter op-type payload context))
      (fresh-context)
      operations)))

(defn txdata-op
  "Implementation for :chimera.datomic-peer.operation/txdata"
  [adapter _ payload context]
  (update context :txdata concat
    (:chimera.datomic-peer.operation.txdata/edn payload)))
