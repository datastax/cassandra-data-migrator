package com.datastax.cdm.cql.codec;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class TEXT_LongCodecTest {

    private TEXT_LongCodec codec;
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        codec = new TEXT_LongCodec( null);
    }

    @Test
    void getJavaType_ShouldReturnLongType() {
        Assertions.assertEquals(GenericType.LONG, codec.getJavaType());
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
        String valueAsString = "9223372036854775807";
        Long value = Long.valueOf(valueAsString);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        ByteBuffer result = codec.encode(value, CqlConversion.PROTOCOL_VERSION);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "9223372036854775807";
        Long value = Long.valueOf(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, CqlConversion.PROTOCOL_VERSION);

        Long result = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        Long value = 9223372036854775807L;
        String expected = TypeCodecs.BIGINT.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "9223372036854775807";
        Long expected = TypeCodecs.BIGINT.parse(valueAsString);
        Long result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
