(ns phaser.builder
  (:require [phaser
             [dsl :as dsl]
             [kahn :as kahn]]
            [phaser.disruptor :as disruptor]
            [clojure.set :as set]))

(def empty-queue clojure.lang.PersistentQueue/EMPTY)

(defn- find-start [handlers graph]
  (let [handlers (set (keys handlers))
        with-incoming (apply set/union (vals graph))]
    (set/difference handlers with-incoming)))

(defn- get-incoming [h graph]
  (seq (reduce-kv (fn [incoming i o]
                    (if (o h)
                      (conj incoming i)
                      incoming))
                  #{} graph)))

(defn build-disruptor [disruptor handlers graph]
  (let [graph (reduce-kv (fn [acc k v]
                           (assoc acc k (set v)))
                         {} graph)
        start-set (find-start handlers graph)
        coverage (set/difference (set (keys handlers))
                                 (apply set/union start-set (vals graph)))]
    (when (seq coverage)
      (throw (ex-info "Unknown handler in graph" {:missing-handlers coverage})))
    (when (empty? start-set)
      (throw (ex-info "No starting nodes in graph" {:graph graph})))
    (doseq [n (kahn/kahn-sort graph)]
      (if-let [incoming (get-incoming n graph)]
        (do
          (println "Adding" n "after" incoming)
          (dsl/handle-events-with (apply dsl/after disruptor (map handlers incoming))
                                 (handlers n)))
        (do
          (println "Adding" n "to start")
          (dsl/handle-events-with disruptor (handlers n)))))
    disruptor))
