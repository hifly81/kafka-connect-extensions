package org.hifly.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
        // Schema for the record's value
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("json_data", Schema.STRING_SCHEMA)
                .build();

        // Record with the json_data field containing JSON
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

        SinkRecord transformedRecord = transformation.apply(record);

        Struct transformedValue = (Struct) transformedRecord.value();

        assertNotNull(transformedValue);

        // The NEW struct WON'T have "id" or "name": it has only JSON fields
        assertEquals("nestedValue", transformedValue.getString("nestedField"));
        assertEquals(42, transformedValue.getInt32("numberField"));

        Struct addFieldStruct = (Struct) transformedValue.get("addField");
        assertNotNull(addFieldStruct);
        assertEquals("Gio", addFieldStruct.getString("name"));

        // Optional: check that id/name are NOT in the resulting struct
        assertThrows(org.apache.kafka.connect.errors.DataException.class, () -> transformedValue.getInt32("id"));

        System.out.println("KEY:" + transformedRecord.key());
        System.out.println("VALUE:" + transformedRecord.value());
    }
}