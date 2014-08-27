(ns serf.command
  (:require [lamina.core :refer [enqueue]]
            [msgpack.core :refer [pack]]))

(def seqs (ref {}))

(defn- get-seq
  "Get the current seq and increment the seq counter for this client."
  [client]
  (dosync
   (let [current (@seqs client)]
     (if current
       (commute seqs assoc client (inc current))
       (commute seqs assoc client 0))
     (or current 0))))

(defn- request-header [client command]
  {"Command" (name command)
   "Seq" (get-seq client)})

(defn make-request
  ([header] (byte-array (pack header)))
  ([header body] (byte-array (concat (pack header) (pack body)))))

(defmulti send-command
  "Send commands through a connected client. Returns the seq of the request."
  (fn [client command & args] command))

(defmethod send-command :handshake [client command]
  (let [header (request-header client command)
        body {"Version" 1}]
    (enqueue client (make-request header body))
    (header "Seq")))

(defmethod send-command :members [client command]
  (let [header (request-header client command)]
    (enqueue client (make-request header))
    (header "Seq")))

(defmethod send-command :event [client command event payload coalesce]
  (let [header (request-header client command)
        body {"Name" event "Payload" payload "Coalesce" coalesce}]
    (enqueue client (make-request header body))
    (header "Seq")))

(defmethod send-command :query [client command name payload timeout-ns]
  (let [header (request-header client command)
        body {"Name" name "Payload" payload "Timeout" timeout-ns}]
    (enqueue client (make-request header body))
    (header "Seq")))
