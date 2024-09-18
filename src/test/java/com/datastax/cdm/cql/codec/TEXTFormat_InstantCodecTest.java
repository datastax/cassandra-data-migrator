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

class TEXTFormat_InstantCodecTest {

    SparkConf sc;
    private TEXTFormat_InstantCodec codec;
    PropertyHelper propertyHelper;

    private static final String FORMAT = "yyMMddHHmmss";
    private static final String TIMEZONE = "Europe/Dublin";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
    ZoneOffset zoneOffset = ZoneId.of(TIMEZONE).getRules().getOffset(Instant.now());

    @BeforeEach
    void setUp() {
        sc = new SparkConf();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        codec = new TEXTFormat_InstantCodec(propertyHelper);
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
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(value, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsInstant() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        Instant result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsText() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        String result = codec.format(value);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsInstant() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        Instant result = codec.parse(valueAsString);
        Assertions.assertEquals(value, result);
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenInvalidFormatString() {
        PropertyHelper.destroyInstance();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "INVALID_FORMAT");
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TEXTFormat_InstantCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenEmptyFormatString() {
        PropertyHelper.destroyInstance();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "");
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TEXTFormat_InstantCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenInvalidTimeZone() {
        PropertyHelper.destroyInstance();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "INVALID_TIMEZONE");
        propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TEXTFormat_InstantCodec(propertyHelper));
    }

    @Test
    void constructor_ShouldThrowIllegalArgumentException_WhenEmptyTimeZone() {
        PropertyHelper.destroyInstance();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "");
        propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        Assertions.assertThrows(IllegalArgumentException.class, () -> new TEXTFormat_InstantCodec(propertyHelper));

    }

}
