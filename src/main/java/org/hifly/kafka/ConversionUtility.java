package org.hifly.kafka;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.connect.data.SchemaAndValue;
import oracle.sql.RAW;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConversionUtility {

    private static final String KEY = "_id";
    private static final Logger log = LoggerFactory.getLogger(ConversionUtility.class);

    public static SchemaAndValue oracleRawToBson(byte[] value) {

        String input ;

        try {
            String raw = new String(value);
            // case when a RAW is already set
            if (raw.startsWith("HEXTORAW")) {

                Pattern pattern = Pattern.compile("HEXTORAW\\('([^']*)'\\)");
                Matcher matcher = pattern.matcher(raw);
                if (matcher.find()) {
                    raw = matcher.group(1);
                    input = raw.toUpperCase();

                    log.debug("EXISTING RAW {}", input);
                } else {
                    log.error("Error in extracting a RAW value - SchemaAndValue.NULL will be returned");
                    return SchemaAndValue.NULL;
                }
            }
            // case when a new RAW must be generated from the hex byte array
            else {
                try {
                    input = new RAW(value).stringValue();

                    log.debug("GENERATED RAW {}", input);
                } catch (Exception rawException) {
                    log.error("Error in generating a RAW value - SchemaAndValue.NULL will be returned", rawException);
                    return SchemaAndValue.NULL;
                }
            }
        } catch (Exception ex) {
            log.error("Generic Error - SchemaAndValue.NULL will be returned", ex);
            return SchemaAndValue.NULL;
        }

        return generateBsonDocument(input);
    }

    /*
     Oracle RAW byte [] size must be 16
     */
    public static byte[] convertToOracleRaw(UUID uuid) {
        String uuidString = uuid.toString().replace("-", "").toUpperCase();
        String finalValue = "";
        finalValue += uuidString.substring(6, 8);
        finalValue += uuidString.substring(4, 6);
        finalValue += uuidString.substring(2, 4);
        finalValue += uuidString.substring(0, 2);
        finalValue += uuidString.substring(10, 12);
        finalValue += uuidString.substring(8, 10);
        finalValue += uuidString.substring(14, 16);
        finalValue += uuidString.substring(12, 14);
        finalValue += uuidString.substring(16, 18);

        finalValue += uuidString.substring(18, uuidString.length());

        try {
            return Hex.decodeHex(finalValue);
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }

    }

    public static String printOracleRaw(String base64) {
        byte[] rawBytes = java.util.Base64.getDecoder().decode(base64);
        RAW rawValue = new RAW(rawBytes);
        return rawValue.stringValue();

    }

    private static SchemaAndValue generateBsonDocument(String input) {
        try {
            Document doc = new Document(KEY, input);
            String json = doc.toJson();

            log.debug("Bson document for json {}", json);

            return new SchemaAndValue(null, json);
        } catch (Exception ex) {
            log.error("Error in generating a Bson document - SchemaAndValue.NULL will be returned", ex);
            return SchemaAndValue.NULL;
        }
    }
}