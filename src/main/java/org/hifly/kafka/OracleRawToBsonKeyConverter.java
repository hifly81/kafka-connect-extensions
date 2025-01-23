package org.hifly.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OracleRawToBsonKeyConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(OracleRawToBsonKeyConverter.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new DataException("Not valid for source connectors!");
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            return BsonUtility.oracleRawToBson(value);
        } catch (Exception e) {
            throw new DataException(e.getMessage());
        }
    }

}