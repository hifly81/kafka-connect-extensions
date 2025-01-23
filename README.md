# Overview

Custom Kafka Connect Converters and SMTs.

## Converters:

 - _org.hifly.kafka.OracleRawToBsonKeyConverter_ - Convert a _byte []_ to _Oracle RAW_ data type. Oracle RAW is then used to create MongoDB Bson document.
 - _org.hifly.kafka.ByteArrayAndStringConverter_ - pass through for byte array schema type and string schema type.

## SMT:

- _org.hifly.kafka.smt.JsonKeyToValue_ - get value from massage _record.key_ and copy on a new field in message _record.value_
- _org.hifly.kafka.smt.ExplodeJsonString_ - extract json value from a massage field and copy the json fields in the message as top-level fields


## Install oracle jdbc driver in maven local repo

```bash
mvn install:install-file -Dfile=ojdbc10.jar -DgroupId=com.oracle -DartifactId=ojdbc10 -Dversion=19.3 -Dpackaging=jar
```

## Execute tests

```bash
mvn clean test
```

## Build and create distributable jar

```bash
mvn clean compile assembly:single
```

## Appendix

### MongoDB Sink Connector - properties

```json
{
  "name" : "mongo-sample-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "XXXXXXXX",
    "topics": "XXXXXXXX",
    "key.converter": "org.hifly.kafka.OracleRawToBsonKeyConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "database": "XXXXXXXX",
    "collection": "XXXXXXXX",
    "errors.tolerance": "all",
    "mongo.errors.log.enable": "true",
    "delete.on.null.values": "true",
    "document.id.strategy.overwrite.existing": "true",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
    "delete.writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy",
    "publish.full.document.only": "true",
    "transforms": "ReplaceField, addKeyToValue, tsConverter1",
    "transforms.ReplaceField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.ReplaceField.exclude": "ID,XXXXXXXX,XXXXXXXX",
    "transforms.addKeyToValue.type": "org.hifly.kafka.smt.JsonKeyToValue",
    "transforms.addKeyToValue.valuename": "ID",
    "transforms.addKeyToValue.idkey": "_id",
    "transforms.tsConverter1.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.tsConverter1.target.type": "dd/MM/yyyy",
    "transforms.tsConverter1.field": "START_DATE"
  }
}

```
