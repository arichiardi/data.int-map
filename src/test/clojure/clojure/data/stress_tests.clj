(ns clojure.data.stress-tests
  (:refer-clojure :exclude [merge merge-with update])
  (:require [clojure.string :as string :refer [trim blank? split join]]
            [clojure.java.io :as io :refer [reader]]
            [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [clojure.data.int-map :as di]))

(declare parse-lines)
(declare parse-file)
(declare parse-lines)

(def ^{:dynamic true} *input*)

(defn load-data-fixture [f]
  (binding [*input* (parse-file "src/test/clojure/clojure/data/samples/ks_10000_0")]
    (f)))

(use-fixtures :once load-data-fixture)

(deftest int-map-update
  (let [{:keys [item-count capacity items]} *input*
        weights (distinct (map second items))]

    (is (= (count weights) (count (loop [m (transient (di/int-map)) is items]
                                  (if (seq is)
                                    (let [i (first is)]
                                      (recur (di/update! m (second i) (fn [_] )) (rest is)))
                                    (persistent! m))))) "Should fill TransientIntMap with update!")

    (is (= (count weights) (count (loop [m (di/int-map) is items]
                                  (if (seq is)
                                    (let [i (first is)]
                                      (recur (di/update m (second i) (fn [_] )) (rest is)))
                                    m)))) "Should fill PersistentIntMap with update")
    ))

(declare compute-column)

(deftest int-map-compute-column
  (let [{:keys [item-count capacity items]} *input*
        necessary-ks (r/fold clojure.core/merge (r/map #(hash-map %1 (lazy-seq (range 1 (inc capacity)))) items))
        [index item] (first (map-indexed #(vector %1 %2) items))]
    (compute-column capacity necessary-ks item index (di/int-map))))

(defn parse-lines
  "Recursive parsing function accepting lines in the format v<space>w and
  returning a vector of [v w] vectors"
  ([line-seq max-lines] (parse-lines line-seq max-lines 0 []))
  ([line-seq max-lines current-line result]
   (if (and (seq line-seq) (< current-line max-lines))
     (let [[vx wx] (split (first line-seq) #"[\s\t]")]
       (recur (rest line-seq) max-lines (inc current-line) (conj result (vector (Long/parseLong vx) (Long/parseLong wx)))))
     result)))

(defn- parse-file
  "Returns the following input map:
  {:item-count n, :capacity K,
   :items [[v_0 w_0] [v_1 w_1] ... [v_n-1 w_n-1]]}"
  [file-name]
  (with-open [rdr (reader file-name)]
    (let [lines (line-seq rdr)
          first-line (split (first lines) #"[\s\t]")
          item-count (Long/parseLong (first first-line))
          capacity (Long/parseLong (second first-line))
          rest-of-lines (rest lines)]
      {:item-count item-count
       :capacity capacity
       :items (parse-lines rest-of-lines item-count)})))

(defn- ^{:author "Andrea Richiardi"}
  compute-cell
  [prev-column item ^long i ^long k]
  [i k])

(defn- ^{:author "Andrea Richiardi" } compute-column
  "A column is represented as a map from k to cell."
  ([capacity] (transient (di/int-map)))
  ([capacity item-k-map item i prev-column] (compute-column capacity
                                                            (get item-k-map item)
                                                            item i prev-column
                                                            (compute-column capacity)))
  ([capacity ks item i prev-column new-column]
   (if (seq ks)
     (let [^long k (first ks)
           ^clojure.lang.PersistentVector cell (compute-cell prev-column item i k)]
       ;; recurring with the new k from the map item->necessary ks
       (recur capacity (rest ks) item i prev-column (di/update! new-column k (fn [_] cell))))
     (persistent! new-column))))
