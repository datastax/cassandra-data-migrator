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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

public class BLOB_TextCodecTest {
    private final String INPUT = "Encode this Text string to Blob";

    private BLOB_TEXTCodec codec;

    @BeforeEach
    public void setup() {
        codec = new BLOB_TEXTCodec(null);
    }

    @Test
    public void testEncode() {
        ByteBuffer buffer = codec.encode(INPUT, ProtocolVersion.V4);
        String retBuffer = codec.decode(buffer, ProtocolVersion.V4);
        assertEquals(INPUT, retBuffer);
    }

    @Test
    void testFormat() {
        ByteBuffer bb = ByteBuffer.wrap(INPUT.getBytes());
        String expected = TypeCodecs.BLOB.format(bb);

        String result = codec.format(INPUT);
        Assertions.assertEquals(expected, result);
    }

}
