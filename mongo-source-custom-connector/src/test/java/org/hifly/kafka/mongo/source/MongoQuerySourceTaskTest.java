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
 * Per eseguire i test in locale:
 *
 * docker run --rm -p 27017:27017 mongo:6.0
 */
class MongoQuerySourceTaskTest {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB = "testdb";
    private static final String COLL_MISURE = "misure";
    private static final String COLL_MISURE_DETTAGLIO = "misure-dettaglio";
    private static final String TIME_FIELD = "lastUpdateTime";

    private MongoClient client;

    private MongoQuerySourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MONGO_URI);
        client.getDatabase(DB).drop();

        // Mock Kafka Connect context
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
        cfg.put(MongoQuerySourceConfig.MONGO_COLLECTION_CONFIG, COLL_MISURE);
        cfg.put(MongoQuerySourceConfig.TOPIC_CONFIG, "test-topic");
        cfg.put(MongoQuerySourceConfig.TIME_FIELD_CONFIG, TIME_FIELD);
        cfg.put(MongoQuerySourceConfig.POLL_INTERVAL_MS_CONFIG, "10");
        cfg.put(MongoQuerySourceConfig.OUTPUT_FORMAT_CONFIG, outputFormat);
        cfg.put(MongoQuerySourceConfig.BASE_FILTER_CONFIG, "{}");
        return cfg;
    }

    @Test
    void testPollWithoutPipeline() throws Exception {

        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        MongoCollection<Document> misure =
                client.getDatabase(DB).getCollection(COLL_MISURE);

        Date t1 = new Date(System.currentTimeMillis() - 1_000);
        Date t2 = new Date(System.currentTimeMillis());

        misure.insertMany(Arrays.asList(
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
    void testPollWithPipeline_MisureAndMisureDettaglio() throws Exception {

        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        MongoCollection<Document> misure =
                client.getDatabase(DB).getCollection(COLL_MISURE);
        MongoCollection<Document> misureDettaglio =
                client.getDatabase(DB).getCollection(COLL_MISURE_DETTAGLIO);

        Date tsWithDetail = new Date(System.currentTimeMillis() - 2_000);
        misure.insertOne(new Document("_id", "RID-CON-DET")
                .append("dettaglioMisuraRif", "RID-0607842-2013-01-1-001")
                .append("dataMisuraRif", new Date())
                .append(TIME_FIELD, tsWithDetail)
                .append("gblIdTestata", "C59FD926-TEST"));

        misureDettaglio.insertOne(new Document("_id", "RID-DET-1")
                .append("dettaglioMisuraRif", "RID-0607842-2013-01-1-001"));

        Date tsNoDetail = new Date(System.currentTimeMillis() - 1_000);
        misure.insertOne(new Document("_id", "RID-SENZA-DET")
                .append("dettaglioMisuraRif", "RID-1490570-2024-01-1-001")
                .append("dataMisuraRif", new Date())
                .append(TIME_FIELD, tsNoDetail)
                .append("gblIdTestata", "TEST-2"));

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);

        cfg.put(MongoQuerySourceConfig.KEY_FIELD_CONFIG, "MisuraRif_Utilizzata");

        String pipelineJson =
                "[{\"$lookup\":{" +
                        "\"from\":\"misure-dettaglio\"," +
                        "\"localField\":\"dettaglioMisuraRif\"," +
                        "\"foreignField\":\"dettaglioMisuraRif\"," +
                        "\"as\":\"d\"," +
                        "\"pipeline\":[{\"$limit\":1}]" +
                        "}}," +
                        "{\"$match\":{\"d\":{\"$size\":0}}}," +
                        "{\"$project\":{" +
                        "\"_id\":1," +
                        "\"dettaglioMisuraRif\":1," +
                        "\"dataInizioMese\":{\"$dateToString\":{\"format\":\"%Y-%m-%d\",\"date\":\"$dataMisuraRif\"}}," +
                        "\"idTestata\":\"$gblIdTestata\"," +
                        "\"MisuraRif_Utilizzata\":{\"$substr\":[\"$dettaglioMisuraRif\",4,{\"$strLenCP\":\"$dettaglioMisuraRif\"}]}" +
                        "}}" +
                        "]";

        cfg.put(MongoQuerySourceConfig.PIPELINE_CONFIG, pipelineJson);

        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(1, records.size());

        SourceRecord r = records.get(0);
        String valueJson = (String) r.value();

        System.out.println("Source Record: key and value -->" + r.key() + "-" + r.value());

        assertTrue(valueJson.contains("RID-1490570-2024-01-1-001"));
        assertFalse(valueJson.contains("RID-0607842-2013-01-1-001"));

        String key = (String) r.key();
        assertNotNull(key);
        assertEquals("1490570-2024-01-1-001", key);
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

    @Test
    void testDocumentWithoutTimeFieldIsEmittedWithTsZeroInOffset() throws Exception {
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        MongoCollection<Document> misure =
                client.getDatabase(DB).getCollection(COLL_MISURE);

        misure.insertOne(new Document("_id", new org.bson.types.ObjectId())
                .append("foo", "bar"));  // nessun TIME_FIELD

        Map<String, String> cfg =
                baseConfig(MongoQuerySourceConfig.OUTPUT_FORMAT_JSON);
        task.start(cfg);

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(1, records.size());

        SourceRecord r = records.get(0);

        Map<?, ?> offset = (Map<?, ?>) r.sourceOffset();
        assertTrue(offset.containsKey("lastProcessedTs"));
        assertEquals(0L, ((Number) offset.get("lastProcessedTs")).longValue());
    }
}