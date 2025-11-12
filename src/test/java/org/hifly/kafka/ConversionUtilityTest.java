package org.hifly.kafka;

import oracle.sql.RAW;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ConversionUtilityTest {

    private static final String KEY = "_id";
    private static final String RAW_DEFAULT_VALUE = "00000000000000000000000000000000";
    private static final int RAW_BYTE_SIZE = 16;

    @Test
    public void testOracleRawToBson () {
        byte [] b1 = ConversionUtility.convertToOracleRaw(UUID.randomUUID());
        Assert.assertEquals(b1.length, RAW_BYTE_SIZE);
        SchemaAndValue result = ConversionUtility.oracleRawToBson(b1);
        String value = commonValidators(result);
        JSONObject obj = new JSONObject(value);
        Assert.assertNotEquals(RAW_DEFAULT_VALUE, obj.get(KEY));

    }

    @Test
    public void testOracleRawNullToBson () {
        byte [] b1 = new byte[RAW_BYTE_SIZE];
        SchemaAndValue result = ConversionUtility.oracleRawToBson(b1);
        String value = commonValidators(result);
        JSONObject obj = new JSONObject(value);
        Assert.assertEquals(RAW_DEFAULT_VALUE, obj.get(KEY));
    }

    @Test
    public void testStringToOracleRaw () {
        RAW raw1 = ConversionUtility.oracleRawFromBase64("QJqZ6oyvgFbgQwqgAKeAVg==");
        RAW raw2 = ConversionUtility.oracleRawFromBase64("R03X4+ygcETgQwqgAKdwRA==");
        RAW raw3 = ConversionUtility.oracleRawFromBase64("W03X4+ygcETgQwqgAKdwZA==");

        System.out.println(raw1.stringValue());

        Assert.assertEquals(raw1.getBytes().length, RAW_BYTE_SIZE);
        Assert.assertEquals(raw2.getBytes().length, RAW_BYTE_SIZE);
        Assert.assertEquals(raw3.getBytes().length, RAW_BYTE_SIZE);

    }

    private static String commonValidators(SchemaAndValue result) {
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.value());
        Assert.assertEquals(result.value().getClass().getName(), String.class.getName());
        String doc = (String)result.value();
        System.out.println(doc);
        return doc;
    }
}
