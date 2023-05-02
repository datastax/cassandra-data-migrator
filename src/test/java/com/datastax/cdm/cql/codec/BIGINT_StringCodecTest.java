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

class BIGINT_StringCodecTest {

    private BIGINT_StringCodec codec;

    @BeforeEach
    void setUp() {
        codec = new BIGINT_StringCodec(null,null);
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
    void getCqlType_ShouldReturnIntType() {
        Assertions.assertEquals(DataTypes.BIGINT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "9223372036854775807";
        Long value = Long.parseLong(valueAsString);
        ByteBuffer expected = TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(valueAsString, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "9223372036854775807";
        Long value = Long.parseLong(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void format_ShouldFormatValueAsString() {
        String valueAsString = "9223372036854775807";
        Long value = Long.parseLong(valueAsString);
        String expected = TypeCodecs.BIGINT.format(value);

        String result = codec.format(valueAsString);
        Assertions.assertEquals(expected, result);
    }

    @Test
        // The test seems trivial because we are basically sending in a
        // number converted to a string, expecting it to convert that to a number
        // and return us the number as a string
    void parse_ShouldParseStringToValueAndReturnAsString() {
        String valueAsString = "9223372036854775807";
        String result = codec.parse(valueAsString);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
        // Slightly more interesting test, we are sending in a string that is not
        // a number, expecting it throw a IllegalArgumentException
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a number";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}

