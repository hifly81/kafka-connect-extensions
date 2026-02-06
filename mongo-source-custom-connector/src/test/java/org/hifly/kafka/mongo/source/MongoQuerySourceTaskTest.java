package org.hifly.kafka.mongo.source;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

/**
 docker run --rm -p 27017:27017 mongo:6.0
 */
class MongoQuerySourceTaskTest {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB = "testdb";
    private static final String COLL = "testcoll";
    private static final String TIME_FIELD = "lastUpdateTime";

    private MongoClient client;
    private MongoCollection<Document> coll;

    private MongoQuerySourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MONGO_URI);
        client.getDatabase(DB).drop();
        coll = client.getDatabase(DB).getCollection(COLL);

        // Mocks Kafka Connect
        context = Mockito.mock(SourceTaskContext.class);
        offsetStorageReader = Mockito.mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);

        task = new MongoQuerySourceTask();
        task.initialize(context);
    }

    @AfterEach
    void tearDown() {
        task.stop();
        client.close();
    }

    private Map<String, String> baseConfig(String outputFormat) {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(MongoQuerySourceConfig.MONGO_URI_CONFIG, MONGO_URI);
        cfg.put(MongoQuerySourceConfig.MONGO_DB_CONFIG, DB);
        cfg.put(MongoQuerySourceConfig.MONGO_COLLECTION_CONFIG, COLL);
        cfg.put(MongoQuerySourceConfig.TOPIC_CONFIG, "test-topic");
        cfg.put(MongoQuerySourceConfig.TIME_FIELD_CONFIG, TIME_FIELD);
        cfg.put(MongoQuerySourceConfig.POLL_INTERVAL_MS_CONFIG, "10");
        cfg.put(MongoQuerySourceConfig.OUTPUT_FORMAT_CONFIG, outputFormat);
        cfg.put(MongoQuerySourceConfig.BASE_FILTER_CONFIG, "{}");
        return cfg;
    }

    @Test
    void testFirstPollFullScanJson() throws Exception {
        // No offset --> first run
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        Date t1 = new Date(System.currentTimeMillis() - 1_000);
        Date t2 = new Date(System.currentTimeMillis());

        coll.insertMany(Arrays.asList(
                new Document("_id", new org.bson.types.ObjectId())
                        .append(TIME_FIELD, t1)
                        .append("value", 1),
                new Document("_id", new org.bson.types.ObjectId())
                        .append(TIME_FIELD, t2)
                        .append("value", 2)
        ));

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(2, records.size());

        for (SourceRecord r : records) {
            assertEquals("test-topic", r.topic());
            assertEquals(org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                    r.valueSchema());
            assertTrue(r.value() instanceof String);

            Map<?, ?> offset = (Map<?, ?>) r.sourceOffset();
            assertTrue(offset.containsKey("lastProcessedTs"));
        }
    }

    @Test
    void testIncrementalPollUsesLastProcessedTs() throws Exception {
        long baseTs = System.currentTimeMillis() - 10_000;

        Date oldDate   = new Date(baseTs - 5_000); // old
        Date inRange1  = new Date(baseTs + 1_000); // ok
        Date inRange2  = new Date(baseTs + 2_000); // ok

        coll.insertMany(Arrays.asList(
                new Document("_id", new org.bson.types.ObjectId())
                        .append(TIME_FIELD, oldDate)
                        .append("value", 0),
                new Document("_id", new org.bson.types.ObjectId())
                        .append(TIME_FIELD, inRange1)
                        .append("value", 1),
                new Document("_id", new org.bson.types.ObjectId())
                        .append(TIME_FIELD, inRange2)
                        .append("value", 2)
        ));

        // previous = baseTs
        Map<String, Object> savedOffset = new HashMap<>();
        savedOffset.put("lastProcessedTs", baseTs);
        when(offsetStorageReader.offset(anyMap())).thenReturn(savedOffset);

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(2, records.size());

        Set<Integer> values = new HashSet<>();
        for (SourceRecord r : records) {
            String json = (String) r.value();
            if (json.contains("\"value\" : 1") || json.contains("\"value\": 1")) {
                values.add(1);
            }
            if (json.contains("\"value\" : 2") || json.contains("\"value\": 2")) {
                values.add(2);
            }
        }
        assertEquals(Set.of(1, 2), values);
    }

    @Test
    void testOutputFormatAvro() throws Exception {
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        Date t = new Date();
        Document d = new Document("_id", new org.bson.types.ObjectId())
                .append(TIME_FIELD, t)
                .append("foo", "bar");
        coll.insertOne(d);

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_AVRO);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(1, records.size());

        SourceRecord r = records.get(0);
        assertTrue(r.value() instanceof org.apache.kafka.connect.data.Struct);

        org.apache.kafka.connect.data.Struct s =
                (org.apache.kafka.connect.data.Struct) r.value();

        assertEquals(d.getObjectId("_id").toHexString(), s.getString("_id"));
        assertEquals(t.getTime(), s.getInt64("timestamp"));

        String payload = s.getString("payload");
        assertNotNull(payload);
        assertTrue(payload.contains("\"foo\""));
    }

    @Test
    void testDocumentWithoutTimeFieldIsSkipped() throws Exception {
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        coll.insertOne(new Document("_id", new org.bson.types.ObjectId())
                .append("foo", "bar"));

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        // Nessun record prodotto
        assertNotNull(records);
        assertEquals(0, records.size());
    }

    @Test
    void testEmptyCollectionReturnsEmptyList() throws Exception {
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(0, records.size());
    }
}