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

class TEXTMillis_InstantCodecTest {

    private TEXTMillis_InstantCodec codec;

    @BeforeEach
    void setUp() {
        codec = new TEXTMillis_InstantCodec(null);
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
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeInstantToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(value, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsInstant() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        Instant result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsText() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String result = codec.format(value);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsInstant() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        Instant result = codec.parse(valueAsString);
        Assertions.assertEquals(value, result);
    }
}
