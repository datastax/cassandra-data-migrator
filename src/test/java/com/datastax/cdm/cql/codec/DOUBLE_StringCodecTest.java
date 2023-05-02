package com.datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class DOUBLE_StringCodecTest {

    private DOUBLE_StringCodec codec;
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        codec = new DOUBLE_StringCodec(null,null);
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.STRING, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnIntType() {
        Assertions.assertEquals(DataTypes.DOUBLE, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String stringValue = "21474836470.7";
        Double value = Double.parseDouble(stringValue);
        ByteBuffer expected = TypeCodecs.DOUBLE.encode(value, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "21474836470.7";
        Double value = Double.parseDouble(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.DOUBLE.encode(value, ProtocolVersion.DEFAULT);

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        // We do not really need to preserve the original format; we cannot predict
        // how many decimal places will be in the original value, so we will allow
        // the codec to use Double's built-in string formatter.
        Assertions.assertEquals(String.valueOf(value), result);
    }

    @Test
        // The test seems trivial because we are basically sending in a
        // number converted to a string, expecting it to convert that to a number
        // and return us the number as a string
    void parse_ShouldParseStringToValueAndReturnAsString() {
        String valueAsString = "21474836470.7";
        // We do not really need to preserve the original format; we cannot predict
        // how many decimal places will be in the original value, so we will allow
        // the codec to use Double's built-in string formatter.
        Double value = Double.parseDouble(valueAsString);
        String result = codec.format(valueAsString);
        Assertions.assertEquals(String.valueOf(value), result);
    }

    @Test
        // Slightly more interesting test, we are sending in a string that is not
        // a number, expecting it throw a IllegalArgumentException
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a number";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}

