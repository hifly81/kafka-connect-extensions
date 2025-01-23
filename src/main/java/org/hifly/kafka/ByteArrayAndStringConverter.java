package org.hifly.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ByteArrayAndStringConverter implements Converter, HeaderConverter {

    private static final ConfigDef CONFIG_DEF = ConverterConfig.newConfigDef();
    private static final Logger log = LoggerFactory.getLogger(ByteArrayAndStringConverter.class);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES) {
            if (schema.type() == Schema.Type.STRING && value instanceof String result) {
                log.debug("Received a string for KEY {} - its own byte [] representation with default charset will be sent", value);
                return result.getBytes();
            }
        }

        if (value != null && !(value instanceof byte[]))
            throw new DataException("ByteArrayConverter is not compatible with objects of type " + value.getClass());

        return (byte[]) value;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }

    @Override
    public void close() {
    }
}