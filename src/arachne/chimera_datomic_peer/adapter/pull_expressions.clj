(ns arachne.chimera-datomic-peer.adapter.pull-expressions
  (:require [arachne.core.config :as cfg]
            [arachne.chimera.adapter :as a]))

(defn- pull-expr-for-ref
  "Returns a pull expression for a reference to a type (i.e, a pull expression
   containing only the types key attribute)"
  [adapter type]
  [(a/key-for-type adapter type)])

(defn- pull-expr-for-type
  "Build a pull expression for the given entity type in the specified adapter"
  [adapter type]
  (let [cfg (:arachne/config adapter)
        attrs (cfg/q cfg '[:find [?attr ...]
                           :in $ ?domain ?adapter
                           :where
                           [?adapter :chimera.adapter/model ?a]
                           [?a :chimera.attribute/domain ?domain]
                           [?a :chimera.attribute/name ?attr]]
                type (:db/id adapter))
        primitives (filter #(not (a/ref? adapter %)) attrs)
        components (filter #(a/component? adapter %) attrs)
        refs (filter #(and (a/ref? adapter %)
                           (not (a/component? adapter %))) attrs)]
    (apply vector
      (merge
        (zipmap
          components
          (map #(pull-expr-for-type adapter (a/attr-range adapter %)) components))
        (zipmap
          refs
          (map #(pull-expr-for-ref adapter (a/attr-range adapter %)) refs)))
        primitives)))

(defn build-pull-expressions
  "Build an index of pull expressions for all the key attributes in an
   adapter, based on the adapter's data model."
  [adapter]
  (let [keytypes (cfg/q (:arachne/config adapter)
                   '[:find ?attr ?type
                     :in $ ?adapter
                     :where
                     [?adapter :chimera.adapter/model ?dme]
                     [?dme :chimera.attribute/key true]
                     [?dme :chimera.attribute/name ?attr]
                     [?dme :chimera.attribute/domain ?type]]
                   (:db/id adapter))]
    (zipmap (map first keytypes)
            (map #(pull-expr-for-type adapter (second %)) keytypes))))
