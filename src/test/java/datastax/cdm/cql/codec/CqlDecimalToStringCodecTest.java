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


class CqlDecimalToStringCodecTest {

    private CqlDecimalToStringCodec codec;

    @BeforeEach
    void setUp() {
        codec = new CqlDecimalToStringCodec(null,null);
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.STRING, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnDecimalType() {
        Assertions.assertEquals(DataTypes.DECIMAL, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String stringValue = "123.456";
        BigDecimal decimalValue = new BigDecimal(stringValue);

        ByteBuffer expectedByteBuffer = TypeCodecs.DECIMAL.encode(decimalValue, ProtocolVersion.DEFAULT);
        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);

        Assertions.assertEquals(expectedByteBuffer, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToDecimalValueAndReturnAsString() {
        String stringValue = "123.456";
        BigDecimal decimalValue = new BigDecimal(stringValue);

        ByteBuffer byteBuffer = TypeCodecs.DECIMAL.encode(decimalValue, ProtocolVersion.DEFAULT);
        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);

        Assertions.assertEquals(stringValue, result);
    }

    @Test
    void format_ShouldFormatDecimalValueAsString() {
        String stringValue = "123.456";
        BigDecimal decimalValue = new BigDecimal(stringValue);

        String result = codec.format(stringValue);
        Assertions.assertEquals(decimalValue.toString(), result);
    }

    @Test
    void parse_ShouldParseStringToDecimalValueAndReturnAsString() {
        String stringValue = "123.456";
        BigDecimal decimalValue = new BigDecimal(stringValue);

        String result = codec.parse(stringValue);
        Assertions.assertEquals(decimalValue.toString(), result);
    }
}
