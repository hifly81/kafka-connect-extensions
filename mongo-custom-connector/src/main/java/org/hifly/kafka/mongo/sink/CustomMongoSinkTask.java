package org.hifly.kafka.mongo.sink;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonArray;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CustomMongoSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(CustomMongoSinkTask.class);


    private MongoCollection<BsonDocument> collection;
    private String arrayField;
    private String idField;
    private String dedupArrayFieldKeys;
    private MongoClient mongoClient;

    @Override
    public void start(Map<String, String> props) {
        mongoClient = MongoClients.create(props.get("connection.uri"));
        collection = mongoClient
            .getDatabase(props.get("database"))
            .getCollection(props.get("collection"), BsonDocument.class);
        arrayField = props.get("doc.array.field.name");
        idField = props.get("doc.id.name");
        dedupArrayFieldKeys = props.get("doc.array.field.dedup.keys");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {

            // Tombstone message: value is null, delete doc
            if (record.value() == null) {
                String keyRaw = record.key().toString();
                String keyResult = getKeyResult(keyRaw);
                BsonDocument idDoc = new BsonDocument(idField, new BsonString(keyResult));
                log.debug("Delete doc with id {}", keyResult);
                collection.deleteOne(Filters.eq("_id", idDoc));
                continue;
            }

            BsonDocument valueDoc = convertToBsonDocument(record.value());
            String keyRaw = record.key().toString();
            String keyResult = getKeyResult(keyRaw);
            BsonDocument idDoc = new BsonDocument(idField, new BsonString(keyResult));

            if (!valueDoc.containsKey(arrayField)) {
                log.warn("Message does not contain element {} - It will be skipped", arrayField);
                continue;
            }

            BsonValue newArrayElement = valueDoc.get(arrayField);
            BsonDocument existingDoc = collection.find(Filters.eq("_id", idDoc)).first();

            if (existingDoc != null) {
                log.debug("Existing doc found {}", existingDoc);

                // Copy/merge all fields except 'arrayField'
                for (String field : valueDoc.keySet()) {
                    if (!field.equals(arrayField)) {
                        existingDoc.put(field, valueDoc.get(field));
                    }
                }

                // Get or init arrayField
                BsonArray arrayFieldArray = existingDoc.getArray(arrayField, new BsonArray());
                arrayFieldArray.add(newArrayElement);

                // Dedup logic: keep only unique keyFields pairs, keeping the LAST occurrence
                String[] keyFields = dedupArrayFieldKeys.split("\\s*,\\s*");

                Map<String, BsonDocument> latestMap = new LinkedHashMap<>();
                for (BsonValue arrayElementValue : arrayFieldArray) {
                    if (!arrayElementValue.isDocument()) continue;
                    BsonDocument arrayElementDoc = arrayElementValue.asDocument();

                    // Build the composite key using any fields in keyFields
                    StringBuilder keyBuilder = new StringBuilder();
                    for (String field : keyFields) {
                        if (keyBuilder.length() > 0) keyBuilder.append("::");
                        keyBuilder.append(arrayElementDoc.containsKey(field) ? arrayElementDoc.get(field).toString() : "");
                    }
                    String key = keyBuilder.toString();
                    latestMap.put(key, arrayElementDoc); // last wins
                }

                BsonArray dedupeArrayField = new BsonArray();
                dedupeArrayField.addAll(latestMap.values());
                existingDoc.put(arrayField, dedupeArrayField);

                log.debug("Replace document {}", idDoc);
                collection.replaceOne(Filters.eq("_id", idDoc), existingDoc);

            } else {
                // New doc: all fields from incoming, "arrayField" as array
                valueDoc.put("_id", idDoc);
                BsonArray arrayFieldArray = new BsonArray();
                arrayFieldArray.add(newArrayElement);
                valueDoc.put(arrayField, arrayFieldArray);
                log.debug("Insert a new document {}", valueDoc);
                collection.insertOne(valueDoc);
            }
        }
    }

    private static String getKeyResult(String keyRaw) {
        String keyResult;
        if (keyRaw.startsWith("Struct") && keyRaw.contains("_id=")) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("_id=\"([^\"]+)\"").matcher(keyRaw);
            if (matcher.find()) {
                keyResult = matcher.group(1);
            } else {
                keyResult = keyRaw;
            }
        } else {
            keyResult = keyRaw;
        }
        return keyResult;
    }

    @Override
    public void stop() {
        if (mongoClient != null) mongoClient.close();
    }

    private BsonDocument convertToBsonDocument(Object value) {
        if (value instanceof Map) {
            return BsonDocument.parse(new org.bson.Document((Map<String, ?>) value).toJson());
        }
        if (value instanceof BsonDocument) {
            return (BsonDocument) value;
        }
        throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}