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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ProtocolVersion;

public class TEXT_BLOBCodecTest {

    private TEXT_BLOBCodec codec;

    @BeforeEach
    public void setup() {
        codec = new TEXT_BLOBCodec(null);
    }

    @Test
    public void encodeDecode() {
        ByteBuffer buffer = codec.encode(ByteBuffer.wrap("Encode this Text string to Blob".getBytes()),
                ProtocolVersion.V4);
        ByteBuffer retBuffer = codec.decode(buffer, ProtocolVersion.V4);
        assertEquals("'Encode this Text string to Blob'", codec.format(retBuffer));
        assertEquals(retBuffer, codec.parse("Encode this Text string to Blob"));
    }

}
