# Kafka Connect Extensions

A collection of custom Kafka Connect components providing additional connectors, converters, and Single Message Transformations (SMTs) for Apache Kafka.

## 📋 Overview

This repository contains custom Kafka Connect add-ons designed to extend Kafka Connect capabilities with specialized functionality for MongoDB integration, Oracle data type conversion, and JSON message transformation.

**Components:**
- **Connectors** - Custom sink connectors for external systems
- **Converters** - Data type converters for specialized formats
- **SMTs** - Single Message Transformations for record manipulation

## ✨ Features

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

### Converters

#### OracleRawToBsonKeyConverter
**Class:** `org.hifly.kafka.OracleRawToBsonKeyConverter`

Converts Oracle RAW data type (`byte[]`) to BSON format, enabling seamless integration between Oracle databases and MongoDB through Kafka Connect.

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

## 🔧 Prerequisites

- **Java**: JDK 11 or later (main project), JDK 17 or later (mongo-custom-connector)
- **Maven**: 3.6.0 or later
- **Apache Kafka**: 3.5.0+ (for connectors and SMTs)
- **MongoDB**: 4.x or later (for CustomMongoSinkConnector)
- **Oracle JDBC Driver**: 19.3 (for OracleRawToBsonKeyConverter)

## 🚀 Building the Project

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

The output JAR will be located at: `target/kafka-connect-extensions-1.9.1.jar`

#### 3. Run Tests

```bash
mvn clean test
```

---

### MongoDB Custom Connector

The MongoDB custom connector is a separate module with its own build configuration.

#### 1. Build the Connector

```bash
cd mongo-custom-connector

# Compile the project
mvn clean compile

# Create distribution package
mvn clean compile assembly:single
```

The output will be located at: `mongo-custom-connector/target/mongo-custom-sink-0.1.0.zip`

#### 2. Run Tests

```bash
cd mongo-custom-connector
mvn clean test
```

## 📦 Installation

### Installing Converters & SMTs

1. Build the main project JAR as described above
2. Copy the JAR to your Kafka Connect plugin path:

```bash
cp target/kafka-connect-extensions-1.9.1.jar $KAFKA_HOME/libs/
```

3. Restart Kafka Connect workers

### Installing MongoDB Connector

1. Build the connector package as described above
2. Extract the distribution:

```bash
unzip mongo-custom-connector/target/mongo-custom-sink-0.1.0.zip -d $KAFKA_CONNECT_PLUGINS/
```

3. Restart Kafka Connect workers

## 📖 Usage Examples

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

## 🏗️ Project Structure

```
kafka-connect-extensions/
├── src/main/java/org/hifly/kafka/
│   ├── smt/
│   │   ├── JsonKeyToValue.java
│   │   └── ExplodeJsonString.java
│   ├── OracleRawToBsonKeyConverter.java
│   ├── ByteArrayAndStringConverter.java
│   └── ConversionUtility.java
├── mongo-custom-connector/
│   └── src/main/java/org/hifly/kafka/mongo/sink/
│       ├── CustomMongoSinkConnector.java
│       └── CustomMongoSinkTask.java
├── pom.xml
└── README.md
```

## 🔬 Running the Classes

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

### Testing MongoDB Connector

The MongoDB connector can be tested using the unit tests included in the project, or deployed to a running Kafka Connect cluster as shown above.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## 📄 License

This project is available under the MIT License.

## 🔗 Links

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [MongoDB Connector Guide](https://docs.mongodb.com/kafka-connector/)

## 📧 Support

For issues, questions, or contributions, please visit the GitHub repository.