# Kafka Connect Extensions

A collection of custom Kafka Connect components providing additional connectors, converters, and Single Message Transformations (SMTs) for Apache Kafka.

## Overview

This repository contains custom Kafka Connect add-ons designed to extend Kafka Connect capabilities with specialized functionality for MongoDB integration, Oracle data type conversion, and JSON message transformation.

**Components:**
- **Connectors**
    - Custom **sink** connector for MongoDB
    - Custom **source** connector for MongoDB (incremental polling + aggregation pipeline)
- **Converters** - Data type converters for specialized formats
- **SMTs** - Single Message Transformations for record manipulation
- **Write Model Strategies** - Custom MongoDB write strategies for conditional upserts

## Features

### Connectors

#### CustomMongoSinkConnector
**Class:** `org.hifly.kafka.mongo.sink.CustomMongoSinkConnector`

A custom MongoDB sink connector with advanced features:
- **Upsert Operations**: Insert or update documents based on document ID
- **Delete Support**: Handle tombstone messages (null values) for document deletion
- **Array Field Management**: Append elements to array fields within documents
- **Deduplication**: Automatic deduplication of array elements based on configurable key fields
- **Field Merging**: Intelligent merging of document fields during updates

**Use Cases:**
- Maintaining aggregated data in MongoDB with unique array elements
- CDC (Change Data Capture) scenarios requiring upserts and deletes
- Event sourcing with array-based event logs

---

#### CustomMongoSourceConnector (Incremental MongoDB Source)

**Class:** `org.hifly.kafka.mongo.source.MongoQuerySourceConnector`

A custom MongoDB **source connector** that periodically polls a MongoDB collection and publishes results to a Kafka topic, with support for:

- **Incremental fetch over time intervals**
    - `time.field`: name of the timestamp/datetime field (for example `lastUpdateDate`)
    - It reads documents where `time.field` is in the range `(lastProcessedTs, now]`
    - The Kafka Connect offset stores the last processed timestamp (`lastProcessedTs`)

- **Simple query or Aggregation Pipeline**
    - `mongo.base.filter`  
      JSON string with a Mongo filter document (for example `{"status": "ACTIVE"}`), used with `find(...)`
    - `mongo.pipeline`  
      JSON string representing an **array of aggregation stages** (for example `[{"$match": {...}}, {"$project": {...}}]`).  
      If configured, the task uses `collection.aggregate(pipeline + time-window $match)` instead of `find`.

- **Output format**
    - `output.format=json`  
      Kafka value is a **schemaless JSON string** (`Schema.STRING_SCHEMA`)
    - `output.format=avro`  
      Kafka value is an Avro `Struct` with a predefined schema (for example `_id`, `payload`, `timestamp`)

- **Kafka key selection**
    - `mongo.key.field`  
      Name (optionally a dotted path) of the Mongo document field to use as the **Kafka key** (string)
    - If not set, the default key is the document `_id`


**Use Cases:**
- Incremental polling of a collection/table based on a timestamp field
- Enrichment on MongoDB side with `$lookup`, `$project`, `$group`, and other aggregation stages
- Publishing to Kafka **only documents that have not been processed yet** (idempotent / incremental behavior)



#### MongoDB Write Model Strategies
**Class:** `org.hifly.kafka.mongo.writestrategy.UpdateIfNewerByDateStrategy`

A custom MongoDB write model strategy for the MongoDB Kafka Sink that performs conditional upserts based on a logical update timestamp field (e.g. lastModificationDate):
- **Conditional Upsert**: Performs an UpdateOne with upsert=true only whenthe target document does not exist, or the incoming date value is more recent than the one stored in MongoDB
- **No Overwrite on Older Data**: If the existing document has a newer date value than the incoming record, the update is a no-op (the document is left unchanged).

**Use Cases:**
- CDC pipelines where events can arrive out of order and you must avoid overwriting newer state with older events.
- Snapshot + incremental update scenarios where the date represents the business last-update timestamp.
- Integrations where MongoDB must always reflect the most recent logical version of a record, not just the last processed event.

---

### Converters

#### OracleRawToBsonKeyConverter
**Class:** `org.hifly.kafka.OracleRawToBsonKeyConverter`

Converts byte arrays containing Oracle RAW data to BSON format, enabling seamless integration between Oracle databases and MongoDB through Kafka Connect.

**Use Cases:**
- Oracle CDC to MongoDB replication
- Converting Oracle RAW keys to BSON ObjectIds

#### ByteArrayAndStringConverter
**Class:** `org.hifly.kafka.ByteArrayAndStringConverter`

A pass-through converter supporting both byte array and string schema types, providing flexible data handling for keys and values.

**Use Cases:**
- Mixed data type handling in heterogeneous systems
- Header conversion scenarios

---

### Single Message Transformations (SMTs)

#### JsonKeyToValue
**Class:** `org.hifly.kafka.smt.JsonKeyToValue`

Adds the message key to the message value as a new field, useful for denormalization and data enrichment.

**Configuration:**
- `valuename`: Field name to add the key to
- `idkey`: Name of the ID field to compute

**Use Cases:**
- Embedding record keys in message bodies
- Simplifying downstream processing by denormalizing data

#### ExplodeJsonString
**Class:** `org.hifly.kafka.smt.ExplodeJsonString`

Extracts JSON content from a string field and promotes nested JSON fields to top-level fields in the record, creating a proper Struct representation.

**Configuration:**
- `valuename`: Name of the JSON field to extract and explode

**Use Cases:**
- Flattening nested JSON structures
- Converting JSON strings to structured data for downstream processing
- Preparing data for systems that don't handle nested JSON well

## Prerequisites

- **Java**: JDK 11 or later (main project)
- **Maven**: 3.6.0 or later
- **Apache Kafka**: 3.5.0+ (for connectors and SMTs)
- **MongoDB**: 4.x or later (for CustomMongoSinkConnector)
- **Oracle JDBC Driver**: 19.3 (for OracleRawToBsonKeyConverter)

## Building the Project

### Main Project (Converters & SMTs)

The main project contains the converters and SMTs. Note that it requires the Oracle JDBC driver to be installed locally.

#### 1. Install Oracle JDBC Driver

Download the Oracle JDBC driver (`ojdbc10.jar`) and install it to your local Maven repository:

```bash
mvn install:install-file \
  -Dfile=ojdbc10.jar \
  -DgroupId=com.oracle \
  -DartifactId=ojdbc10 \
  -Dversion=19.3 \
  -Dpackaging=jar
```

#### 2. Build the Project

```bash
# Compile the project
mvn clean compile

# Create the package (shaded JAR with dependencies)
mvn clean package
```

The output JAR will be located at: `target/kafka-connect-extensions-<version>.jar`

#### 3. Run Tests

```bash
mvn clean test
```

---

### MongoDB Custom Connectors

The MongoDB custom connectors are in separate modules with their own build configurations.

#### 1. Build the Connector

```bash
cd mongo-custom-connector
cd mongo-source-custom-connector

# Compile the project
mvn clean compile

# Create distribution package
mvn clean compile assembly:single
```

The output will be located at: 
    - `mongo-custom-connector/target/mongo-custom-sink-<version>.zip`
    - `mongo-source-custom-connector/target/mongo-source-poll-<version>.zip`

#### 2. Run Tests

```bash
cd mongo-custom-connector
mvn clean test
```

## Installation

### Installing Converters & SMTs

1. Build the main project JAR as described above
2. Copy the JAR to your Kafka Connect plugin path:

```bash
cp target/kafka-connect-extensions-<version>.jar $KAFKA_CONNECT_PLUGIN_PATH
```

3. Restart Kafka Connect workers

### Installing MongoDB Connectors

1. Build the connector packages as described above
2. Extract the distribution:

```bash
unzip mongo-custom-connector/target/mongo-custom-sink-<version>.zip -d $KAFKA_CONNECT_PLUGINS/
```

```bash
unzip mongo-source-custom-connector/target/mongo-source-poll-<version>.zip -d $KAFKA_CONNECT_PLUGINS/
```

3. Restart Kafka Connect workers

## Usage Examples

### Using JsonKeyToValue SMT

```json
{
  "name": "my-connector",
  "config": {
    "connector.class": "...",
    "transforms": "addKey",
    "transforms.addKey.type": "org.hifly.kafka.smt.JsonKeyToValue",
    "transforms.addKey.valuename": "recordId",
    "transforms.addKey.idkey": "id"
  }
}
```

### Using ExplodeJsonString SMT

```json
{
  "name": "my-connector",
  "config": {
    "connector.class": "...",
    "transforms": "explode",
    "transforms.explode.type": "org.hifly.kafka.smt.ExplodeJsonString",
    "transforms.explode.valuename": "jsonData"
  }
}
```

### Using CustomMongoSinkConnector

```json
{
  "name": "mongo-sink-custom",
  "config": {
    "connector.class": "org.hifly.kafka.mongo.sink.CustomMongoSinkConnector",
    "tasks.max": "1",
    "topics": "your-topic",
    "connection.uri": "mongodb://localhost:27017",
    "database": "your-database",
    "collection": "your-collection",
    "doc.array.field.name": "events",
    "doc.id.name": "userId",
    "doc.array.field.dedup.keys": "eventId,timestamp",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

### Using CustomMongoSourceConnector

```json
{
  "name": "mongo-query-source-json",
  "config": {
    "connector.class": "com.example.connect.mongo.MongoQuerySourceConnector",
    "tasks.max": "1",
    "topic": "mongo.pipeline.output",
    "mongo.uri": "mongodb://user:pass@host:27017/db",
    "mongo.database": "mydb",
    "mongo.collection": "orders",
    "time.field": "lastUpdateDate",
    "mongo.key.field": "idProduct",
    "poll.interval.ms": "60000",
    "output.format": "json",

    "mongo.pipeline": "[ \
{\"$lookup\": { \
\"from\": \"order.details\", \
\"localField\": \"idProduct\", \
\"foreignField\": \"idProduct\", \
\"as\": \"d\", \
\"pipeline\": [ { \"$limit\": 1 } ] \
}}, \
{\"$match\": { \"d\": { \"$size\": 0 } }}, \
{\"$project\": { \
\"_id\": 1, \
\"idProduct\": 1 } \
} ] \
} \
}} \
]"
}
}
```

### Using UpdateIfNewerByDataAggiornamentoStrategy

```json
{
  "name": "mongo-sink-conditional-upsert",
  "config": {
    "connector.class": "org.hifly.kafka.mongo.writestrategy.UpdateIfNewerByDateStrategy",
    "tasks.max": "1",
    "topics": "your-topic",
    "connection.uri": "mongodb://localhost:27017",
    "database": "your-database",
    "collection": "your-collection",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy",
    "document.id.strategy.overwrite.existing": "true",
    "upsert.date.field.name": "lastUpdateTs",
    "post.processor.chain": "com.mongodb.kafka.connect.sink.processor.DocumentIdAdder",
    "writemodel.strategy": "org.hifly.kafka.mongo.writestrategy.UpdateIfNewerByDataAggiornamentoStrategy",
    "delete.on.null.values": "true",
    "delete.writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy"
  }
}
```

### Using OracleRawToBsonKeyConverter

```json
{
  "name": "oracle-to-mongo",
  "config": {
    "connector.class": "...",
    "key.converter": "org.hifly.kafka.OracleRawToBsonKeyConverter"
  }
}
```

##  Running the Classes

### Testing Converters and SMTs

The converters and SMTs are designed to run within Kafka Connect. To test them:

1. Build the project
2. Configure a Kafka Connect connector with the desired component
3. Deploy to Kafka Connect (Standalone or Distributed mode)

**Standalone Mode:**
```bash
$KAFKA_HOME/bin/connect-standalone.sh \
  config/connect-standalone.properties \
  config/your-connector.properties
```

**Distributed Mode:**
```bash
# Start Kafka Connect
$KAFKA_HOME/bin/connect-distributed.sh config/connect-distributed.properties

# Deploy connector via REST API
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @your-connector-config.json
```

### Testing MongoDB Connectors

The MongoDB connectors can be tested using the unit tests included in the project and a local mongodb instance running, or deployed to a running Kafka Connect cluster as shown above.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is available under the MIT License.

## Links

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [MongoDB Connector Guide](https://docs.mongodb.com/kafka-connector/)

## Support

For issues, questions, or contributions, please visit the GitHub repository.