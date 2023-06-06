package com.datastax.cdm.cql.codec;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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
        Assertions.assertEquals(value,result);
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
