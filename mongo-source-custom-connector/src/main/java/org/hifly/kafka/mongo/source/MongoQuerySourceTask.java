package org.hifly.kafka.mongo.source;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Aggregates;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.BsonArray;
import org.bson.BsonValue;

import java.util.*;

public class MongoQuerySourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MongoQuerySourceTask.class);
    private static final Long INITIAL_TS = 0L;

    private MongoClient client;
    private MongoCollection<Document> collection;
    private MongoQuerySourceConfig config;

    private String topic;
    private String timeField;
    private long pollIntervalMs;
    private long lastPollTime;

    // Offset: last processed timestamp (epoch millis)
    private Long lastProcessedTs;

    private enum OutputFormat { JSON, AVRO }

    private OutputFormat outputFormat;
    private Schema keySchema;
    private Schema valueSchema;

    private List<Bson> basePipeline;
    private String pipelineJson;
    private String keyField;

    private static final Schema AVRO_VALUE_SCHEMA = SchemaBuilder.struct()
            .name("MongoRecord")
            .field("_id", Schema.STRING_SCHEMA)
            .field("payload", Schema.STRING_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .build();

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new MongoQuerySourceConfig(props);

        String uri = config.mongoUri();
        String dbName = config.database();
        String collName = config.collection();
        this.topic = config.topic();
        this.timeField = config.timeField();
        this.pollIntervalMs = config.pollIntervalMs();

        this.client = MongoClients.create(uri);
        this.collection = client.getDatabase(dbName).getCollection(collName);
        this.lastPollTime = System.currentTimeMillis();
        this.keyField = config.keyField();

        // output format
        String fmt = config.outputFormat().toLowerCase(Locale.ROOT);
        if (MongoQuerySourceConfig.OUTPUT_FORMAT_AVRO.equals(fmt)) {
            this.outputFormat = OutputFormat.AVRO;
            this.keySchema = Schema.STRING_SCHEMA;
            this.valueSchema = AVRO_VALUE_SCHEMA;
        } else {
            this.outputFormat = OutputFormat.JSON;
            this.keySchema = Schema.STRING_SCHEMA;
            this.valueSchema = Schema.STRING_SCHEMA;
        }

        this.pipelineJson = config.pipelineJson();
        this.basePipeline = parsePipeline(pipelineJson);

        // Read previous offset
        Map<String, Object> partition = sourcePartition(dbName, collName);
        Map<String, Object> offset = context.offsetStorageReader().offset(partition);

        if (offset != null && offset.get("lastProcessedTs") != null) {
            Object tsObj = offset.get("lastProcessedTs");
            if (tsObj instanceof Long) {
                this.lastProcessedTs = (Long) tsObj;
            } else if (tsObj instanceof Integer) {
                this.lastProcessedTs = ((Integer) tsObj).longValue();
            } else if (tsObj instanceof String) {
                this.lastProcessedTs = Long.parseLong((String) tsObj);
            } else {
                log.warn("{}. Ignore offset.",
                        tsObj.getClass());
                this.lastProcessedTs = INITIAL_TS;
            }
        } else {
            this.lastProcessedTs = INITIAL_TS; // first run
        }

        log.info("MongoQuerySourceTask started. topic={}, db={}, coll={}, timeField={}, " +
                        "pollIntervalMs={}, outputFormat={}, lastProcessedTs={}",
                topic, dbName, collName, timeField, pollIntervalMs, outputFormat, lastProcessedTs);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTime;

        if (elapsed < pollIntervalMs) {
            long sleepMs = pollIntervalMs - elapsed;
            log.debug("Sleeping {} ms before next poll", sleepMs);
            Thread.sleep(sleepMs);
        }
        lastPollTime = System.currentTimeMillis();
        long windowEnd = lastPollTime;

        String baseFilterJson = config.baseFilterJson();
        Bson baseFilter;
        try {
            baseFilter = Document.parse(baseFilterJson);
        } catch (Exception e) {
            log.warn("Can't parse mongo.base.filter='{}'. Empty filter. Errore: {}",
                    baseFilterJson, e.getMessage());
            baseFilter = new Document();
        }

        Bson timeFilter;

        Date fromExclusive = new Date(lastProcessedTs);
        Date toInclusive = new Date(windowEnd);
        timeFilter = Filters.and(
                baseFilter,
                Filters.gt(timeField, fromExclusive),
                Filters.lte(timeField, toInclusive)
        );

        Iterable<Document> cursor;

        // Usage --> pipeline
        if (!basePipeline.isEmpty()) {
            List<Bson> effectivePipeline = new ArrayList<>(basePipeline);
            effectivePipeline.add(Aggregates.match(timeFilter));
            cursor = collection.aggregate(effectivePipeline);
        } else {
            // No pipeline: fallback a find() + sort come prima
            cursor = collection
                    .find(timeFilter)
                    .sort(Sorts.ascending(timeField));
        }


        List<SourceRecord> records = new ArrayList<>();
        long maxTsInBatch = (lastProcessedTs == null) ? Long.MIN_VALUE : lastProcessedTs;

        for (Document doc : cursor) {
            Object tsObj = doc.get(timeField);
            if (!(tsObj instanceof Date)) {
                // skip...
                continue;
            }
            long ts = ((Date) tsObj).getTime();
            // ... update maxTsInBatch ...

            Object keyValueObj = extractKeyFromDocument(doc);
            String key;
            if (keyValueObj == null) {
                log.warn("mongo.key.field='{}'. Fallback on _id. doc={}",
                        keyField, doc.toJson());
                Object fallbackId = doc.get("_id");
                key = (fallbackId != null) ? String.valueOf(fallbackId) : null;
            } else {
                key = String.valueOf(keyValueObj);
            }

            Map<String, Object> partition = sourcePartition(
                    collection.getNamespace().getDatabaseName(),
                    collection.getNamespace().getCollectionName());
            Map<String, Object> offset = Collections.singletonMap("lastProcessedTs", ts);

            SourceRecord record;
            if (outputFormat == OutputFormat.JSON) {
                String valueJson = doc.toJson();
                record = new SourceRecord(
                        partition,
                        offset,
                        topic,
                        keySchema,    // tipicamente Schema.STRING_SCHEMA
                        key,
                        valueSchema,
                        valueJson
                );
            } else {
                String valueJson = doc.toJson();
                Struct valueStruct = new Struct(AVRO_VALUE_SCHEMA)
                        .put("_id", doc.get("_id") != null ? doc.get("_id").toString() : null)
                        .put("payload", valueJson)
                        .put("timestamp", ts);

                record = new SourceRecord(
                        partition,
                        offset,
                        topic,
                        keySchema,   // STRING_SCHEMA
                        key,
                        valueSchema,
                        valueStruct
                );
            }

            records.add(record);
        }

        if (maxTsInBatch != Long.MIN_VALUE) {
            lastProcessedTs = maxTsInBatch;
            log.info("Poll done: prodotti {} record, new lastProcessedTs={}",
                    records.size(), lastProcessedTs);
        } else {
            log.info("Poll done: no new records.");
        }

        return records;
    }

    @Override
    public void stop() {
        log.info("Stop MongoQuerySourceTask");
        if (client != null) {
            client.close();
        }
    }

    private Map<String, Object> sourcePartition(String db, String coll) {
        Map<String, Object> partition = new HashMap<>();
        partition.put("db", db);
        partition.put("collection", coll);
        return partition;
    }

    private List<Bson> parsePipeline(String json) {
        if (json == null || json.trim().isEmpty() || json.trim().equals("[]")) {
            return Collections.emptyList();
        }
        try {
            BsonArray array = BsonArray.parse(json);
            List<Bson> stages = new ArrayList<>(array.size());
            for (BsonValue v : array) {
                stages.add(v.asDocument());
            }
            return stages;
        } catch (Exception e) {
            log.warn("Can't parse mongo.pipeline='{}'. Empty pipeline. Error: {}",
                    json, e.getMessage());
            return Collections.emptyList();
        }
    }

    private Object extractKeyFromDocument(Document doc) {
        if (keyField == null || keyField.isEmpty()) {
            // fallback: _id
            return doc.get("_id");
        }

        if (!keyField.contains(".")) {
            return doc.get(keyField);
        }

        // Dotted path: es. "customer.id"
        String[] parts = keyField.split("\\.");
        Object current = doc;
        for (String part : parts) {
            if (!(current instanceof Document)) {
                return null;
            }
            current = ((Document) current).get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
}