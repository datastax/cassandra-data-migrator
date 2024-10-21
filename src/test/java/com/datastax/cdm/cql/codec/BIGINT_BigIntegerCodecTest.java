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

import java.math.BigInteger;
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

class BIGINT_BigIntegerCodecTest {

    private BIGINT_BigIntegerCodec codec;

    @BeforeEach
    void setUp() {
        codec = new BIGINT_BigIntegerCodec(null);
    }

    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.BIG_INTEGER, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnIntType() {
        Assertions.assertEquals(DataTypes.BIGINT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        ByteBuffer expected = TypeCodecs.BIGINT.encode(101l, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(BigInteger.valueOf(101l), CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        ByteBuffer byteBuffer = TypeCodecs.BIGINT.encode(101l, CqlConversion.PROTOCOL_VERSION);

        BigInteger result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(BigInteger.valueOf(101l), result);
    }

    @Test
    void testFormat() {
        String valueAsString = "9223372036854775807";
        Long value = Long.parseLong(valueAsString);
        String expected = TypeCodecs.BIGINT.format(value);

        String result = codec.format(BigInteger.valueOf(9223372036854775807l));
        Assertions.assertEquals(expected, result);
    }

    @Test
    void testParse() {
        String valueAsString = "9223372036854775807";
        BigInteger result = codec.parse(valueAsString);
        Assertions.assertEquals(BigInteger.valueOf(9223372036854775807l), result);
    }

    @Test
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a number";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}
