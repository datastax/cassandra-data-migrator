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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

class BigInteger_BIGINTCodecTest {

    private BigInteger_BIGINTCodec codec;

    @BeforeEach
    void setUp() {
        codec = new BigInteger_BIGINTCodec(null);
    }

    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.INTEGER, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnIntType() {
        Assertions.assertEquals(DataTypes.INT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        ByteBuffer expected = TypeCodecs.INT.encode(Integer.valueOf(101), CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(Integer.valueOf(101), CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        ByteBuffer byteBuffer = TypeCodecs.INT.encode(101, CqlConversion.PROTOCOL_VERSION);

        Integer result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(Integer.valueOf(101), result);
    }

    @Test
    void format_ShouldFormatValueAsString() {
        String valueAsString = "999999";
        Long value = Long.parseLong(valueAsString);
        String expected = TypeCodecs.BIGINT.format(value);

        String result = codec.format(Integer.valueOf(valueAsString));
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseStringToValueAndReturnAsString() {
        String valueAsString = "999999";
        Integer result = codec.parse(valueAsString);
        Assertions.assertEquals(Integer.valueOf(valueAsString), result);
    }

    @Test
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a number";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}
