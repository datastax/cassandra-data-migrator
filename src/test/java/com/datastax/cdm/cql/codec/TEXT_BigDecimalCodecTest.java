package com.datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

class TEXT_BigDecimalCodecTest {

    private TEXT_BigDecimalCodec codec;

    @BeforeEach
    void setUp() {
        codec = new TEXT_BigDecimalCodec(null, null);
    }

    @Test
    void getJavaType_ShouldReturnBigDecimalType() {
        Assertions.assertEquals(GenericType.of(BigDecimal.class), codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnTextType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeNumberToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "12345.6789";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(value, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "12345.6789";
        BigDecimal value = new BigDecimal(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        BigDecimal result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(value, result);
    }

    @Test
    void parse_ShouldParseStringToBigDecimalValue() {
        String stringValue = "12345.6789";
        BigDecimal expectedValue = TypeCodecs.DECIMAL.parse(stringValue);
        BigDecimal result = codec.parse(stringValue);
        Assertions.assertEquals(expectedValue, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        BigDecimal value = new BigDecimal("12345.6789");;
        String expectedValue = TypeCodecs.DECIMAL.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expectedValue, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "12345.6789";
        BigDecimal expected = TypeCodecs.DECIMAL.parse(valueAsString);
        BigDecimal result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
