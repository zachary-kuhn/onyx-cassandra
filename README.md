## onyx-core-cassandra

Onyx plugin for cassandra.

#### Installation

In your project file:

```clojure
[onyx-cassandra "0.5.3"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.cassandra])
```

#### Catalog entries

##### sample-entry

```clojure
{:onyx/name :entry-name
 :onyx/ident :cassandra/task
 :onyx/type :input
 :onyx/medium :cassandra
 :onyx/consumption :concurrent
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from cassandra"}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:cassandra/attr`            | `string`  | Description here.

#### Lifecycle Arguments

##### `sample-entry`

```clojure
(defmethod l-ext/inject-lifecycle-resources :cassandra/task
  [_ _] {:cassandra/arg val})
```

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
