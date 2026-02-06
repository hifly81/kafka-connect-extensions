package org.hifly.kafka.mongo.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MongoQuerySourceConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topic";
    public static final String MONGO_URI_CONFIG = "mongo.uri";
    public static final String MONGO_DB_CONFIG = "mongo.database";
    public static final String MONGO_COLLECTION_CONFIG = "mongo.collection";
    public static final String BASE_FILTER_CONFIG = "mongo.base.filter";
    public static final String PIPELINE_CONFIG = "mongo.pipeline";
    public static final String KEY_FIELD_CONFIG = "mongo.key.field";
    public static final String TIME_FIELD_CONFIG = "time.field";
    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    public static final String OUTPUT_FORMAT_CONFIG = "output.format";

    public static final String OUTPUT_FORMAT_JSON = "json";
    public static final String OUTPUT_FORMAT_AVRO = "avro";

    public MongoQuerySourceConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(
                        TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Destination Topic")
                .define(
                        MONGO_URI_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Mongo connection URI")
                .define(
                        MONGO_DB_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Database Mongo")
                .define(
                        MONGO_COLLECTION_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Collection Mongo")
                .define(
                        BASE_FILTER_CONFIG,
                        ConfigDef.Type.STRING,
                        "{}",
                        ConfigDef.Importance.MEDIUM,
                        "Mongo query at every poll")
                .define(
                        PIPELINE_CONFIG,
                        ConfigDef.Type.STRING,
                        "[]",
                        ConfigDef.Importance.MEDIUM,
                        "Pipeline aggregation Mongo JSON array, es: "
                                + "[{\"$match\": {...}}, {\"$project\": {...}}]")
                .define(
                        KEY_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "key Kafka, if not set use _id."
                )
                .define(
                        TIME_FIELD_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "timestamp field for incremental fetch")
                .define(
                        POLL_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        60000L,
                        ConfigDef.Importance.MEDIUM,
                        "polling interval in milliseconds")
                .define(
                        OUTPUT_FORMAT_CONFIG,
                        ConfigDef.Type.STRING,
                        OUTPUT_FORMAT_JSON,
                        ConfigDef.ValidString.in(OUTPUT_FORMAT_JSON, OUTPUT_FORMAT_AVRO),
                        ConfigDef.Importance.MEDIUM,
                        "output format 'json' or 'avro' (Struct Avro)");
    }

    public String topic() {
        return getString(TOPIC_CONFIG);
    }

    public String mongoUri() {
        return getString(MONGO_URI_CONFIG);
    }

    public String database() {
        return getString(MONGO_DB_CONFIG);
    }

    public String collection() {
        return getString(MONGO_COLLECTION_CONFIG);
    }

    public String baseFilterJson() {
        return getString(BASE_FILTER_CONFIG);
    }

    public String pipelineJson() { return getString(PIPELINE_CONFIG); }

    public String keyField() { return getString(KEY_FIELD_CONFIG); }

    public String timeField() {
        return getString(TIME_FIELD_CONFIG);
    }

    public long pollIntervalMs() {
        return getLong(POLL_INTERVAL_MS_CONFIG);
    }

    public String outputFormat() {
        return getString(OUTPUT_FORMAT_CONFIG);
    }
}