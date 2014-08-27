(ns serf.client
  (:require [clojure.core.incubator :refer [dissoc-in]]
            [lamina.core :refer [enqueue wait-for-result]]
            [aleph.tcp :refer [tcp-client]]
            [serf.command :refer [send-command]]))

(def responses (ref {}))

(defn make-client
  "Create an Aleph TCP client for communicating with the Serf agent."
  ([] (make-client {}))
  ([{host :host port :port}]
     (wait-for-result
      (tcp-client {:host (or host "localhost")
                   :port (or port 7373)}))))

(defn- wait-for-done
  "Wait until all responses from this seq have been received."
  [client command request-seq]
  (when (#{:query} command)
    (receive client)
    (let [seq-responses (get-in @responses [client request-seq])]
      (when-not (seq (filter #(= "done" (% "Type")) seq-responses))
        (recur client command request-seq)))))

(defn send-command!
  "Synchronously send a request, wait for the response, and return it."
  [client command & args]
  (let [request-seq (apply send-command client command args)]
    (receive client)
    (wait-for-done client command request-seq)
    (dosync
     (let [response (get-in @responses [client request-seq])]
       (commute responses dissoc-in [client request-seq])
       response))))

(defn- get-response
  "Read from the channel and return responses as a byte stream."
  [buffer]
  (loop [buf buffer
         acc []]
    (if (.readable buffer)
      (recur buffer (conj acc (.readByte buffer)))
      acc)))

(defn- unpack-multiple
  "Unpack multiple msgpack structures from a byte sequence,
  returning them as a seq. Fairly inefficient."
  [byte-stream]
  (loop [stream (get-response byte-stream)
         acc []]
    (if (seq stream)
      (let [obj (unpack stream)
            byte-count (count (pack obj))]
        (recur (drop byte-count stream) (conj acc obj)))
      acc)))

(defn- receive
  "Receive potentially multiple commands from the client connection
  and add them to the responses ref."
  [client]
  (let [received (wait-for-message client 2000)
        response-objs (unpack-multiple received)]
    (loop [response-objs response-objs]
      (let [this-response (first response-objs)
            next-response (second response-objs)]
        (if-not (nil? this-response)
          (if (or (nil? next-response) (next-response "Seq"))
            (do (dosync
                 (commute responses
                          assoc-in
                          [client (this-response "Seq")]
                          [this-response]))
                (recur (rest response-objs)))
            (do (dosync
                 (commute responses
                          assoc-in
                          [client (this-response "Seq")]
                          (conj (or (get-in @responses [client (this-response "Seq")]) []) this-response next-response)))
                (recur (drop 2 response-objs)))))))))
