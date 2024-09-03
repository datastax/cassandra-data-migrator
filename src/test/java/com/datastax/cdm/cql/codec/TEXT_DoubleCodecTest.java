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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

class TEXT_DoubleCodecTest {

    private TEXT_DoubleCodec codec;
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        codec = new TEXT_DoubleCodec(null);
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.DOUBLE, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnDoubleType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeNumberToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "21474836470.7";
        Double value = Double.valueOf(valueAsString);
        // Because the valueAsString could be a Double with a decimal point or with an E notation
        // but we expect the encoded value to be from a proper CQL type, we need to encode the String.valueOf(value)
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(value, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "21474836470.7";
        Double value = Double.valueOf(valueAsString);
        // encoding could be from user input, so may not be in Java Double.toString() format
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        Double result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        Double value = 21474836470.7;
        String expected = TypeCodecs.DOUBLE.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "21474836470.7";
        Double expected = TypeCodecs.DOUBLE.parse(valueAsString);
        Double result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
