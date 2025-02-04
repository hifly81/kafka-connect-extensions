package org.hifly.kafka;

public class OracleRawTest {

    public static void main(String[] args) {
        System.out.println(
                "Oracle RAW value: " + ConversionUtility.printOracleRaw("QJqZ6oyvgFbgQwqgAKeAVg=="));
        System.out.println(
                "Oracle RAW value: " + ConversionUtility.printOracleRaw("R03X4+ygcETgQwqgAKdwRA=="));
        System.out.println(
                "Oracle RAW value: " + ConversionUtility.printOracleRaw("W03X4+ygcETgQwqgAKdwZA=="));
    }
}
