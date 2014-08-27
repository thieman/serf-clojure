# serf

An incomplete Clojure client for Serf

## Usage

```clojure
(ns myapp.core
  (:require [serf.client :refer [make-client send-command!]]))

(def c (make-client {:host "localhost" :port 7373}))

(send-command! c :handshake)

(send-command! c :event "deploy" "a payload" false)
```

## Methods Implemented

* handshake
* members
* event
* query
