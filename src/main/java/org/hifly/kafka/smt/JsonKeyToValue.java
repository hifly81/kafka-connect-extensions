package org.hifly.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class JsonKeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String ROWKEY_CONFIG = "valuename";

    public static final String IDKEY_CONFIG = "idkey";

    public static final String OVERVIEW_DOC = "Add the record key to the value as a field.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ROWKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name to add.")
            .define(IDKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Id name to compute.");

    private static final String PURPOSE = "Add the record key to the value as a field.";

    private static final Logger log = LoggerFactory.getLogger(JsonKeyToValue.class);

    private String rowKey;

    private String idKey;

    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        rowKey = config.getString(ROWKEY_CONFIG);
        idKey = config.getString(IDKEY_CONFIG);
    }

    public R apply(R record) {
        return applySchemaless(record);
    }

    private R applySchemaless(R record) {

        Map<String, Object> value;

        // Tombstone message handling
        if(record.value() == null) {

            // Get the value from a json key
            JSONObject obj = new JSONObject(record.key().toString());
            JSONObject jsonObject = new JSONObject(obj.toString());

            String inner;
            try {
                inner = jsonObject.getString(idKey);
            } catch (Exception ex) {
                JSONObject innerObj = (JSONObject) jsonObject.get(idKey);
                inner = innerObj.getString(idKey);
            }

            String keyValue = "{\"_id\":\""+inner+"\"}";

            log.debug("Tombstone record found for key {}", keyValue);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    keyValue,
                    null,
                    null,
                    null
            );
        }

        if(record.value() instanceof String) {
            String strValue = (String)record.value();
            JSONObject jsonObject = new JSONObject(strValue);
            value = new HashMap<>();
            jsonObject.keys().forEachRemaining(key -> value.put(key, jsonObject.getString(key)));

        }
        else {
            try {
                value = (Map<String, Object>) record.value();
            } catch (Exception e) {
                log.error("Can't parse record.value", e);
                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        null,
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp()
                );
            }
        }

        try {
            // Get the value from a json key
            JSONObject obj = new JSONObject(record.key().toString());
            JSONObject jsonObject = new JSONObject(obj.toString());

            try {
                String inner = jsonObject.getString(idKey);
                value.put(rowKey, inner);
            } catch (Exception ex) {
                JSONObject innerObj = (JSONObject) jsonObject.get(idKey);
                String inner = innerObj.getString(idKey);
                value.put(rowKey, inner);
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    value,
                    record.timestamp()
            );
        } catch (Exception e) {
            log.error("Can't parse " + idKey, e);
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
    }

    private R applyWithSchema(R record) {
        return applySchemaless(record);
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {

    }

}