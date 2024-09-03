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

class DECIMAL_StringCodecTest {

    private DECIMAL_StringCodec codec;

    @BeforeEach
    void setUp() {
        codec = new DECIMAL_StringCodec(null);
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.STRING, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnDecimalType() {
        Assertions.assertEquals(DataTypes.DECIMAL, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "123.456";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer expected = TypeCodecs.DECIMAL.encode(value, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "123.456";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.DECIMAL.encode(value, CqlConversion.PROTOCOL_VERSION);

        String result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void format_ShouldFormatValueAsString() {
        String valueAsString = "123.456";
        BigDecimal value = new BigDecimal(valueAsString);
        String expected = TypeCodecs.DECIMAL.format(value);

        String result = codec.format(valueAsString);
        Assertions.assertEquals(expected, result);
    }

    @Test
    // The test seems trivial because we are basically sending in a
    // number converted to a string, expecting it to convert that to a number
    // and return us the number as a string
    void parse_ShouldParseStringToValueAndReturnAsString() {
        String valueAsString = "123.456";
        String result = codec.parse(valueAsString);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    // Slightly more interesting test, we are sending in a string that is not
    // a number, expecting it throw a IllegalArgumentException
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a number";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}
