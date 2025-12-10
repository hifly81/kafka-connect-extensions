package org.hifly.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

public class ByteArrayAndStringConverterTest {

    private final ByteArrayAndStringConverter converter = new ByteArrayAndStringConverter();

    @Test
    public void testFromConnectDataWithStringSchemaAndValue() {
        String testString = "hello world";
        byte[] bytes = converter.fromConnectData("test-topic", Schema.STRING_SCHEMA, testString);

        Assert.assertNotNull(bytes);
        Assert.assertArrayEquals(testString.getBytes(), bytes);
    }

    @Test
    public void testFromConnectDataWithBytesSchemaAndValue() {
        byte[] testBytes = new byte[] {0x01, 0x02, 0x03};
        byte[] result = converter.fromConnectData("test-topic", Schema.BYTES_SCHEMA, testBytes);

        Assert.assertNotNull(result);
        Assert.assertArrayEquals(testBytes, result);
    }

    @Test
    public void testFromConnectDataWithNullSchemaAndByteValue() {
        byte[] testBytes = new byte[] {0x04, 0x05};
        byte[] result = converter.fromConnectData("another-topic", null, testBytes);

        Assert.assertNotNull(result);
        Assert.assertArrayEquals(testBytes, result);
    }

    @Test
    public void testFromConnectDataWithNullValue() {
        byte[] result = converter.fromConnectData("null-topic", Schema.BYTES_SCHEMA, null);
        Assert.assertNull(result);
    }

    @Test
    public void testFromConnectDataWithNonByteArrayNonStringValue() {
        Integer intValue = 12345;
        byte[] result = converter.fromConnectData("int-topic", Schema.INT32_SCHEMA, intValue);

        Assert.assertNotNull(result);
        Assert.assertArrayEquals(intValue.toString().getBytes(), result);
    }

    @Test
    public void testFromConnectDataWithStructValue() {
        Schema structSchema = SchemaBuilder.struct().name("TestStruct").field("f1", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(structSchema).put("f1", "v1");
        byte[] result = converter.fromConnectData("struct-topic", structSchema, struct);

        Assert.assertNotNull(result);
        Assert.assertArrayEquals(struct.toString().getBytes(), result);
    }

    @Test
    public void testToConnectData() {
        byte[] testBytes = new byte[] {0x11, 0x12};
        SchemaAndValue sv = converter.toConnectData("topic", testBytes);
        Assert.assertNotNull(sv);
        Assert.assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, sv.schema());
        Assert.assertArrayEquals(testBytes, (byte[]) sv.value());
    }

    @Test
    public void testFromConnectHeaderDelegatesToFromConnectData() {
        String testHeader = "header-val";
        byte[] bytes = converter.fromConnectHeader("topic", "headerKey", Schema.STRING_SCHEMA, testHeader);

        Assert.assertNotNull(bytes);
        Assert.assertArrayEquals(testHeader.getBytes(), bytes);
    }

    @Test
    public void testToConnectHeaderDelegatesToToConnectData() {
        byte[] testBytes = new byte[] {0x13, 0x14};
        SchemaAndValue result = converter.toConnectHeader("topic", "headerKey", testBytes);

        Assert.assertNotNull(result);
        Assert.assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, result.schema());
        Assert.assertArrayEquals(testBytes, (byte[]) result.value());
    }
}