/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.cql.codec;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assertions;

public class CodecTestHelper {
    public static void assertByteBufferEquals(ByteBuffer expected, ByteBuffer actual) {
        Assertions.assertEquals(expected.remaining(), actual.remaining(),
                () -> String.format(
                        "ByteBuffers have different remaining bytes:%nExpected byte[]: %s%nActual byte[]: %s",
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
