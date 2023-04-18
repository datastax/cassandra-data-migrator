package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class TEXT_DoubleCodecTest {

    private TEXT_DoubleCodec codec;
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        codec = new TEXT_DoubleCodec(null, null);
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.DOUBLE, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnDoubleType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeNumberToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "21474836470.7";
        Double value = Double.valueOf(valueAsString);
        // Because the valueAsString could be a Double with a decimal point or with an E notation
        // but we expect the encoded value to be from a proper CQL type, we need to encode the String.valueOf(value)
        ByteBuffer expected = TypeCodecs.TEXT.encode(String.valueOf(value), ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(value, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "21474836470.7";
        Double value = Double.valueOf(valueAsString);
        // encoding could be from user input, so may not be in Java Double.toString() format
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        Double result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        Double value = 21474836470.7;
        String expected = TypeCodecs.DOUBLE.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "21474836470.7";
        Double expected = TypeCodecs.DOUBLE.parse(valueAsString);
        Double result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
