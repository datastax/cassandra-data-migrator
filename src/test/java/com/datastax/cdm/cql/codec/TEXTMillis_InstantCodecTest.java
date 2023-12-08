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

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

class TEXTMillis_InstantCodecTest {

    private TEXTMillis_InstantCodec codec;

    @BeforeEach
    void setUp() {
        codec = new TEXTMillis_InstantCodec(null);
    }

    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.INSTANT, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnTimestampType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeInstantToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(value, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsInstant() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        Instant result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsText() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String result = codec.format(value);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsInstant() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        Instant result = codec.parse(valueAsString);
        Assertions.assertEquals(value, result);
    }
}
