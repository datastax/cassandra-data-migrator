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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

class TIMESTAMP_StringFormatCodecTest {

    private TIMESTAMP_StringFormatCodec codec;

    private static final String FORMAT = "yyMMddHHmmss";
    private static final String TIMEZONE = "Europe/Dublin";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
    ZoneOffset zoneOffset = ZoneId.of(TIMEZONE).getRules().getOffset(Instant.now());

    @BeforeEach
    void setUp() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        codec = new TIMESTAMP_StringFormatCodec(propertyHelper);
    }

    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.STRING, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnTimestampType() {
        Assertions.assertEquals(DataTypes.TIMESTAMP, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer expected = TypeCodecs.TIMESTAMP.encode(value, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer byteBuffer = TypeCodecs.TIMESTAMP.encode(value, CqlConversion.PROTOCOL_VERSION);

        String result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsString() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        String expected = TypeCodecs.TIMESTAMP.format(value);

        String result = codec.format(valueAsString);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseStringToValueAndReturnAsString() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        String result = codec.parse(TypeCodecs.TIMESTAMP.format(value));
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldThrowIllegalArgumentException_WhenVFormatIsInvalid() {
        String valueAsString = "not a valid format";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenInvalidFormatString() {
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.setProperty(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "INVALID_FORMAT");

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TIMESTAMP_StringFormatCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenInvalidTimeZone() {
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.setProperty(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "INVALID_TIMEZONE");

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TIMESTAMP_StringFormatCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenEmptyFormatString() {
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.setProperty(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "");

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TIMESTAMP_StringFormatCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenEmptyTimeZone() {
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.setProperty(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "");

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TIMESTAMP_StringFormatCodec(propertyHelper));
    }
}
