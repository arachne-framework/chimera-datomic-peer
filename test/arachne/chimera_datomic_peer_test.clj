(ns arachne.chimera-datomic-peer-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [arachne.core :as core]
            [arachne.core.runtime :as rt]
            [arachne.core.config :as cfg]
            [arachne.core.config.model :as m]
            [arachne.core.dsl :as a]
            [arachne.core.config :as core-cfg]
            [arachne.error :as e]
            [arachne.chimera :as chimera]
            [arachne.chimera.dsl :as ch]
            [arachne.chimera.adapter :as ca]
            [arachne.chimera.operation :as op]
            [arachne.chimera.test-harness :as harness]
            [com.stuartsierra.component :as component]
            [arachne.chimera-datomic-peer.dsl :as dsl]
            [datascript.core :as d])
  (:import [arachne ArachneException]
           (java.util UUID Date)))

(e/explain-test-errors!)

(defn test-adapter
  "Given a migration ref, return a ne"
  [migration]
  (let [uri (str "datomic:mem://" (UUID/randomUUID))]
    (dsl/adapter uri [migration])))

(deftest test-harness
  (harness/exercise-all test-adapter [:org.arachne-framework/chimera-datomic-peer]))

(defn seed-data-config
  "DSL function to build a test config"
  [person-id]

  (ch/migration :test/m1
    "Migration to set up schema for example-based tests"
    []
    (ch/attr :test.person/id :test/Person :key :uuid :min 1 :max 1)
    (ch/attr :test.person/name :test/Person :string :min 1 :max 1))

  (ch/migration :test/m2
    "Migration containing some test seed data"
    [:test/m1]
    (dsl/seed [{:test.person/id person-id
                :test.person/name "Test Person"}]))

  (a/id :test/adapter
    (dsl/adapter (str "datomic:mem://" (UUID/randomUUID))
      [:test/m2]
      :ensure true
      :wipe true))

  (a/id :test/rt (a/runtime [:test/adapter])))

(deftest test-seed-data

  (let [person-id (UUID/randomUUID)
        cfg (core/build-config [:org.arachne-framework/chimera-datomic-peer]
              `(seed-data-config ~person-id))
        rt (rt/init cfg [:arachne/id :test/rt])
        rt (component/start rt)
        adapter (rt/lookup rt [:arachne/id :test/adapter])]
    (is (= {:test.person/id person-id
            :test.person/name "Test Person"}
          (chimera/operate adapter :chimera.operation/get
            (chimera/lookup :test.person/id person-id))))))
