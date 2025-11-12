# Overview

Custom Kafka Connect add-ons:
- Connectors
- Converters 
- SMTs

## Connectors:

- _org.hifly.kafka.mongo.CustomMongoSinkConnector_ - Custom Sink connector for MongoDB implementing upsert operations and deletes.


### Build

Package:

```bash
cd mongo-custom-connector
mvn clean compile assembly:single
```

### Execute tests

```bash
cd mongo-custom-connector
mvn clean test
```

---

## Converters:

 - _org.hifly.kafka.OracleRawToBsonKeyConverter_ - Convert a _byte []_ to _Oracle RAW_ data type. 
 - _org.hifly.kafka.ByteArrayAndStringConverter_ - pass through for byte array schema type and string schema type.

## SMTs:

- _org.hifly.kafka.smt.JsonKeyToValue_ - Add message key to message value as a new field. 
- _org.hifly.kafka.smt.ExplodeJsonString_ - Create a Struct for a JSON Field. Extract JSON value from a massage field and copy the JSON fields in the output message as top-level fields


### Build

Install oracle jdbc driver in maven local repo:

```bash
mvn install:install-file -Dfile=ojdbc10.jar -DgroupId=com.oracle -DartifactId=ojdbc10 -Dversion=19.3 -Dpackaging=jar
```

Package:

```bash
mvn clean compile assembly:single
```

### Execute tests

```bash
mvn clean test
```