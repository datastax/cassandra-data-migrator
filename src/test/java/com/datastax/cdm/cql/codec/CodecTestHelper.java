package com.datastax.cdm.cql.codec;

import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;

public class CodecTestHelper {
    public static void assertByteBufferEquals(ByteBuffer expected, ByteBuffer actual) {
        Assertions.assertEquals(expected.remaining(), actual.remaining(),
                () -> String.format("ByteBuffers have different remaining bytes:%nExpected byte[]: %s%nActual byte[]: %s",
                        byteBufferToHexString(expected), byteBufferToHexString(actual)));

        Assertions.assertTrue(expected.equals(actual),
                () -> String.format("ByteBuffers are not equal:%nExpected byte[]: %s%nActual byte[]: %s",
                        byteBufferToHexString(expected), byteBufferToHexString(actual)));
    }

    private static String byteBufferToHexString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.mark();
        buffer.get(bytes);
        buffer.reset();
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            hexString.append(String.format("%02X ", b));
        }
        return hexString.toString();
    }

}
