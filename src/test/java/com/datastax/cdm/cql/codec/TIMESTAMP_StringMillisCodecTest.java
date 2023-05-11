package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

class TIMESTAMP_StringMillisCodecTest {

    private TIMESTAMP_StringMillisCodec codec;

    @BeforeEach
    void setUp() {
        codec = new TIMESTAMP_StringMillisCodec(null);
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
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer expected = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(valueAsString, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer byteBuffer = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void format_ShouldFormatValueAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String expected = TypeCodecs.TIMESTAMP.format(value);

        String result = codec.format(valueAsString);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseStringToInstantValueAndReturnAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String result = codec.parse(TypeCodecs.TIMESTAMP.format(value));
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a millis timestamp";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}
