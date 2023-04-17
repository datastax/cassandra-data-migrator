package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

class TEXT_IntegerCodecTest {

    private TEXT_IntegerCodec codec;
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        codec = new TEXT_IntegerCodec(null, null);
    }

    @Test
    void getJavaType_ShouldReturnIntType() {
        Assertions.assertEquals(GenericType.INTEGER, codec.getJavaType());
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
        String valueAsString = "10";
        Integer value = Integer.valueOf(valueAsString);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(value, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsNumber() {
        String valueAsString = "10";
        Integer value = Integer.valueOf(valueAsString);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        Integer result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatNumberValueAsText() {
        Integer value = 10;
        String expected = TypeCodecs.INT.format(value);
        String result = codec.format(value);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsNumber() {
        String valueAsString = "10";
        Integer expected = TypeCodecs.INT.parse(valueAsString);
        Integer result = codec.parse(valueAsString);
        Assertions.assertEquals(expected, result);
    }
}
