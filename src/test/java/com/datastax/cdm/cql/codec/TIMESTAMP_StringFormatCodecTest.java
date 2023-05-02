package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.cql.CqlHelper;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TIMESTAMP_StringFormatCodecTest {

    private TIMESTAMP_StringFormatCodec codec;

    private static final String FORMAT = "yyMMddHHmmss";
    private static final String TIMEZONE = "Europe/Dublin";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
    ZoneOffset zoneOffset = ZoneId.of(TIMEZONE).getRules().getOffset(Instant.now());

    @Mock
    private CqlHelper cqlHelper;

    @BeforeEach
    void setUp() {
        cqlHelper = mock(CqlHelper.class);
        when(cqlHelper.isCodecRegistered(any())).thenReturn(false);

        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        codec = new TIMESTAMP_StringFormatCodec(propertyHelper,cqlHelper);
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
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer expected = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(valueAsString, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer byteBuffer = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
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
}
