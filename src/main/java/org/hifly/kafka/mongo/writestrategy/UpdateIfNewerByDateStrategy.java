package org.hifly.kafka.mongo.writestrategy;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;
import com.mongodb.kafka.connect.sink.Configurable;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

public class UpdateIfNewerByDateStrategy implements WriteModelStrategy, Configurable {

    public static final String UPSERT_DATE_FIELD_CONFIG =
            "upsert.date.field.name";

    private String upsertDate;

    @Override
    public void configure(final MongoSinkTopicConfig configuration) {
        Object value = configuration.originals().get(UPSERT_DATE_FIELD_CONFIG);
        if (value instanceof String) {
            String v = ((String) value).trim();
            if (!v.isEmpty()) {
                upsertDate = v;
            }
        }
    }

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {

        BsonDocument vd = document
                .getValueDoc()
                .orElseThrow(() -> new DataException(
                        "Could not build WriteModel, value document was missing unexpectedly"));

        // _id already exists (DocumentIdAdder + FullKeyStrategy)
        BsonValue idValue = vd.get(ID_FIELD);
        if (idValue == null) {
            throw new DataException(
                    "Could not build WriteModel, the `_id` field was missing unexpectedly");
        }

        // new value for upsertDate
        BsonValue newTsValue = vd.get(upsertDate);
        if (newTsValue == null) {
            return new UpdateOneModel<>(
                    new BsonDocument(ID_FIELD, idValue),
                    new BsonDocument("$set", vd),
                    new UpdateOptions().upsert(true)
            );

        }

        //'array OR
        BsonArray orArray = new BsonArray();
        // { upsertDate: { $lt: newTsValue } }
        orArray.add(new BsonDocument(
                upsertDate,
                new BsonDocument("$lt", newTsValue)));
        // {         // { upsertDate: { $lt: newTsValue } }: { $exists: false } }
        orArray.add(new BsonDocument(
                upsertDate,
                new BsonDocument("$exists", BsonBoolean.FALSE)));

        // filter:
        // { _id: idValue,
        //   $or: [
        //     { upsertDate: { $lt: newTsValue } },
        //     { upsertDate: { $exists: false } }
        //   ]
        // }
        BsonDocument filter = new BsonDocument(ID_FIELD, idValue)
                .append("$or", orArray);

        BsonDocument update = new BsonDocument("$set", vd);

        UpdateOptions options = new UpdateOptions().upsert(true);

        return new UpdateOneModel<>(filter, update, options);
    }
}