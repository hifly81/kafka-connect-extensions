package org.hifly.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExplodeJsonString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String ROWKEY_CONFIG = "valuename";
    public static final String OVERVIEW_DOC = "Create a struct for a JSON Field";
    private static final String PURPOSE = "Create a struct for a JSON Field.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ROWKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "JSON Field value to extract and .");

    private static final Logger log = LoggerFactory.getLogger(ExplodeJsonString.class);

    private String rowKey;


    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        rowKey = config.getString(ROWKEY_CONFIG);
    }

    public R apply(R record) {
        return applySchemaless(record);
    }

    private R applySchemaless(R record) {

        // Get the value of the record (it should be a Struct in this case)
        if (record.value() == null) {
            log.info("Value is null, skipping record");
            return record;
        }

        Struct value = (Struct) record.value();

        // Get the json_data field from the record
        String jsonData = value.getString(rowKey);
        if (jsonData == null) {
            log.info("Not found a nested json, return original record");
            return record;  // Return the record unchanged if json_data is null
        }

        try {

            Map<String, Object> result = createStruct(jsonData);

            // Return the transformed record with the new value
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    (Schema) result.get("SCHEMA"),
                    result.get("STRUCT"),
                    record.timestamp());

        } catch (Exception ex) {
            log.error("Error in creating new struct", ex);
            throw new ConnectException("Error in creating new struct", ex);
        }

    }

    public static Map<String, Object> createStruct(String json) throws Exception {

        Map<String,Object> result = new HashMap<>();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(json);

        Schema rootSchema = createSchema(rootNode);
        Struct rootStruct = createStruct(rootSchema, rootNode);

        result.put("SCHEMA", rootSchema);
        result.put("STRUCT", rootStruct);

        return result;
    }

    // Dynamically creates a Schema from a JSON node
    private static Schema createSchema(JsonNode jsonNode) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode value = field.getValue();

            if (value.isObject()) {
                // Nested object
                schemaBuilder.field(fieldName, createSchema(value));
            } else if (value.isArray()) {
                Schema arraySchema = buildDynamicArraySchema(value);
                schemaBuilder.field(fieldName, arraySchema);
            } else if (value.isTextual()) {
                // String value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            } else if (value.isInt()) {
                // Integer value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
            } else if (value.isLong()) {
                // Long value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
            } else if (value.isBoolean()) {
                // Boolean value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (value.isDouble()) {
                // Double value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else if (value.isBigDecimal()) {
                // Big Decimal value
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
            }
            else if (value.isNull()) {
                // Null value, use String schema as default
                schemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }
        return schemaBuilder.build();
    }

    public static Schema buildDynamicArraySchema(JsonNode jsonNode) {
        if (!jsonNode.isArray() || !jsonNode.elements().hasNext()) {
            throw new IllegalArgumentException("Input JsonNode must be a non-empty array.");
        }

        // Get the first element to determine the schema
        JsonNode firstElement = jsonNode.elements().next();

        if (!firstElement.isObject()) {
            throw new IllegalArgumentException("Array elements must be JSON objects.");
        }

        // Build the object schema dynamically
        SchemaBuilder objectSchemaBuilder = SchemaBuilder.struct();
        Iterator<Map.Entry<String, JsonNode>> fields = firstElement.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode value = field.getValue();

            // Determine the schema type based on the JsonNode type
            if (value.isTextual()) {
                objectSchemaBuilder.field(fieldName, Schema.STRING_SCHEMA);
            } else if (value.isInt()) {
                //if value is a number without a decimal but it is only the first element, treat it as a double
                //FIXME
                objectSchemaBuilder.field(fieldName, Schema.FLOAT64_SCHEMA);
            } else if (value.isLong()) {
                objectSchemaBuilder.field(fieldName, Schema.INT64_SCHEMA);
            } else if (value.isDouble() || value.isFloat()) {
                objectSchemaBuilder.field(fieldName, Schema.FLOAT64_SCHEMA);
            } else if (value.isBoolean()) {
                objectSchemaBuilder.field(fieldName, Schema.BOOLEAN_SCHEMA);
            } else if (value.isNull()) {
                objectSchemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA); // Default nullable type
            } else {
                objectSchemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA); // Fallback for complex types
            }
        }


        // Build the array schema
        Schema objectSchema = objectSchemaBuilder.build();
        return SchemaBuilder.array(objectSchema).optional().build();
    }

    public static List<Struct> buildDynamicArrayStruct(Schema schema, JsonNode jsonNode) {

        List<Struct> structArray = new ArrayList<>();
        Schema innerSchema = schema.valueSchema();

        if (jsonNode.isArray()) {
            // Iterate over the elements in the array
            for (JsonNode element : jsonNode) {

                Struct innerStruct = new Struct(innerSchema);
                Iterator<Map.Entry<String, JsonNode>> fields = element.fields();

                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String fieldName = field.getKey();
                    JsonNode fieldValue = field.getValue();

                    if (fieldValue.isTextual()) {
                        innerStruct.put(fieldName, fieldValue.asText());
                    } else if (fieldValue.isInt()) {
                        //FIXME int treated as Double
                        innerStruct.put(fieldName, (double) fieldValue.asInt());
                    } else if (fieldValue.isLong()) {
                        innerStruct.put(fieldName, fieldValue.asLong());
                    } else if (fieldValue.isDouble()) {
                        innerStruct.put(fieldName, fieldValue.asDouble());
                    } else if (fieldValue.isBigDecimal()) {
                        innerStruct.put(fieldName, fieldValue.asDouble());
                    } else if (fieldValue.isBoolean()) {
                        innerStruct.put(fieldName, fieldValue.asBoolean());
                    } else if (fieldValue.isNull()) {
                        innerStruct.put(fieldName, null);
                    }
                }

                structArray.add(innerStruct);

            }
        }

        return structArray;
    }


    // Dynamically creates a Struct from a Schema and a JSON node
    private static Struct createStruct(Schema schema, JsonNode jsonNode) {
        Struct struct = new Struct(schema);

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode value = field.getValue();

            if (value.isObject()) {
                // Nested object
                Struct nestedStruct = createStruct(schema.field(fieldName).schema(), value);
                struct.put(fieldName, nestedStruct);
            } else if (value.isArray()) {
                List<Struct> nestedStruct = buildDynamicArrayStruct(schema.field(fieldName).schema(), value);
                struct.put(fieldName, nestedStruct);
            } else if (value.isTextual()) {
                struct.put(fieldName, value.asText());
            } else if (value.isInt()) {
                struct.put(fieldName, value.asInt());
            } else if (value.isLong()) {
                struct.put(fieldName, value.asLong());
            } else if (value.isDouble()) {
                struct.put(fieldName, value.asDouble());
            } else if (value.isBigDecimal()) {
                struct.put(fieldName, value.asDouble());
            } else if (value.isBoolean()) {
                struct.put(fieldName, value.asBoolean());
            } else if (value.isNull()) {
                struct.put(fieldName, null);
            }
        }
        return struct;
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