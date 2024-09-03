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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

class TEXT_BigDecimalCodecTest {

    private TEXT_BigDecimalCodec codec;

    @BeforeEach
    void setUp() {
        codec = new TEXT_BigDecimalCodec(null);
    }

    @Test
    void getJavaType_ShouldReturnBigDecimalType() {
        Assertions.assertEquals(GenericType.of(BigDecimal.class), codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnTextType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeNumberToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "12345.6789";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(value, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "12345.6789";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        BigDecimal result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(value, result);
    }

    @Test
    void parse_ShouldParseStringToBigDecimalValue() {
        String stringValue = "12345.6789";
        BigDecimal expectedValue = TypeCodecs.DECIMAL.parse(stringValue);
        BigDecimal result = codec.parse(stringValue);
        Assertions.assertEquals(expectedValue, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        BigDecimal value = new BigDecimal("12345.6789");
        ;
        String expectedValue = TypeCodecs.DECIMAL.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expectedValue, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "12345.6789";
        BigDecimal expected = TypeCodecs.DECIMAL.parse(valueAsString);
        BigDecimal result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
