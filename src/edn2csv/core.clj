(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

; Creates a UUID
(defn uuid [] (str (java.util.UUID/randomUUID)))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def semantics-header-line "UUID:ID(Semantics),TotalError:int,:LABEL")
(def parentOf-header-line "UUID:ID(Parent),GeneticOperator:str,UUID:ID(Child),:LABEL")
(def errors-header-line "UUID:ID(Error),ErrorValue:int,Position:int,:LABEL")

;Creates a UUID
(defn uuid [] (str (java.util.UUID/randomUUID)))

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleavingindividual
; problems, so I'm sticking with this approach for now.
(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))

; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'dindividual
; just end up with a big list of nil's.)
(defn print-individual-to-csv
  [csv-file line]
  (as-> line $
    (map $ [:uuid :generation :location])
    (concat $ ["Individual"])
    (apply safe-println csv-file $))
  1)

(defn print-parentOf-to-csv
  [csv-file line]
  (let [parents (get line :parent-uuids)]
    (dorun (map (fn [single-parent]
      (as-> line x
        (assoc x :single-parent single-parent)
        (map x [:single-parent :genetic-operators :uuid])
        (concat x ["PARENT_OF"])
        (apply safe-println csv-file x))) parents))
    1))


(def semantics-map (atom {}))
(def errors-vector (atom []))
(def errors-map (atom {}))

(defn make-semantics-and-errors-map
  [line]
  (let [create-uuid (uuid)]
    (swap! semantics-map assoc (get line :errors) {:total-error (get line :total-error) :uuid create-uuid}))
  )

(defn add-semantics-to-map
  [line]
  (make-semantics-map line)
  1)

(defn print-semantics-to-csv
  [semantics-file]
  (doseq [item @semantics-map]
    (apply safe-println semantics-file item))
  )


(defn create-errors-map
  []
  (doseq [item @semantics-map]
    (let [error-vector [(key item) (:uuid item)]
      (doseq [thing error-vector]
        ())






(defn print-errors-to-csv
  [errors-file]
  (doseq [item (create-errors-vector)]
    ()))



(defn print-all
  [parentOf-file individuals-file line]
  (add-semantics-to-map line)
  (print-parentOf-to-csv parentOf-file line)
  (print-individual-to-csv individuals-file line)
  (create-errors-vector)
  )
; (defn edn->csv-sequential [edn-file csv-file]
;   (with-open [out-file (io/writer csv-file)]
;     (safe-println out-file individuals-header-line)
;     (->>
;       (line-seq (io/reader edn-file))
;       ; Skip the first line because it's not an individual
;       (drop 1)
;       (map (partial edn/read-string {:default individual-reader}))
;       (map (partial print-parentOf-to-csv out-file))
;       (reduce +)
;       )))
;
; (defn edn->csv-pmap [edn-file csv-file]
;   (with-open [out-file (io/writer csv-file)]
;     (safe-println out-file individuals-header-line)
;     (->>
;       (line-seq (io/reader edn-file))
;       ; Skip the first line because it's not an individual
;       (drop 1)
;       (pmap (fn [line]
;         (print-individual-to-csv out-file (edn/read-string {:default individual-reader} line))
;         1))
;       count
;       )))
(defn build-csv-filename
  [edn-filename file-name]
  (str (fs/parent edn-filename)
       "/"
       (fs/base-name edn-filename ".edn")
       (str "_reducers")
       (str "_" file-name ".csv"))
  )

(defn edn->csv-reducers [edn-file]
  (with-open [individuals-file (io/writer (build-csv-filename edn-file "Individuals"))
              parentOf-file (io/writer (build-csv-filename edn-file "ParentOf"))
              semantics-file (io/writer (build-csv-filename edn-file "Semantics"))
              errors-file (io/writer (build-csv-filename edn-file "Errors"))]
    (safe-println individuals-file individuals-header-line)
    (safe-println parentOf-file parentOf-header-line)
    (safe-println semantics-file semantics-header-line)
    (safe-println errors-file errors-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
      (r/map (partial print-all parentOf-file individuals-file))
      (r/fold +)
      )
      (print-semantics-to-csv semantics-file)
      (print-errors-to-csv errors-file)
      ))


(defn -main
  [edn-filename & [strategy]]
  (let [individual-csv-file (build-csv-filename edn-filename strategy)]
    (time
      (condp = strategy
        "reducers" (edn->csv-reducers edn-filename)
        (edn->csv-reducers edn-filename))))
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
