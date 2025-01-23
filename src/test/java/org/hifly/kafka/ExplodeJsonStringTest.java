package org.hifly.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hifly.kafka.smt.ExplodeJsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExplodeJsonStringTest {

    private ExplodeJsonString<SinkRecord> transformation;

    @BeforeEach
    void setup() {
        transformation = new ExplodeJsonString<>();
        Map<String, String> config = new HashMap<>();
        config.put("valuename", "json_data");
        transformation.configure(config);
    }

    @Test
    void testTransformValidJson() {
        // Define the schema for the record's value
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("json_data", Schema.STRING_SCHEMA)
                .build();

        // Create a record with the json_data field containing JSON
        Struct value = new Struct(schema)
                .put("id", 123)
                .put("name", "test")
                .put("json_data", "{\"nestedField\":\"nestedValue\",\"numberField\":42, \"addField\":{\"name\":\"Gio\"}}");

        SinkRecord record = new SinkRecord(
                "test-topic",
                0,
                null,
                null,
                schema,
                value,
                0
        );

        // Apply the transformation
        SinkRecord transformedRecord = transformation.apply(record);

        // Validate the result
        Struct transformedValue = (Struct) transformedRecord.value();
        assertNotNull(transformedValue);
        assertEquals(123, transformedValue.getInt32("id"));
        assertEquals("test", transformedValue.getString("name"));
        assertEquals("nestedValue", transformedValue.getString("nestedField"));
        assertEquals("42", transformedValue.getString("numberField"));

        System.out.println("KEY:" + transformedRecord.key());
        System.out.println("VALUE:" + transformedRecord.value());
    }


}