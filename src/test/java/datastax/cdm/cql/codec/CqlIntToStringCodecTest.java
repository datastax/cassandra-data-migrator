package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

// This test class brought to you by ChatGPT...
class CqlIntToStringCodecTest {

    private CqlIntToStringCodec codec;

    @Mock
    private TypeCodec<Integer> intCodec;

    @Mock
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        codec = new CqlIntToStringCodec();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.STRING, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnIntType() {
        Assertions.assertEquals(DataTypes.INT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeIntValueToByteBuffer_WhenValueIsNotNull() {
        String stringValue = "10";
        int intValue = Integer.parseInt(stringValue);
        byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(intValue);
        byteBuffer.rewind();

        when(intCodec.encode(intValue, ProtocolVersion.DEFAULT)).thenReturn(byteBuffer);
        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(byteBuffer, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToIntValueAndReturnAsString() {
        int intValue = 10;
        byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(intValue);
        byteBuffer.rewind();

        when(intCodec.decode(byteBuffer, ProtocolVersion.DEFAULT)).thenReturn(intValue);
        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(String.valueOf(intValue), result);
    }

    @Test
    void format_ShouldFormatIntValueAsString() {
        int intValue = 10;
        when(intCodec.format(intValue)).thenReturn(String.valueOf(intValue));
        String result = codec.format(String.valueOf(intValue));
        Assertions.assertEquals(String.valueOf(intValue), result);
    }

    @Test
    void parse_ShouldParseStringToIntValueAndReturnAsString() {
        int intValue = 10;
        when(intCodec.parse(String.valueOf(intValue))).thenReturn(intValue);
        String result = codec.parse(String.valueOf(intValue));
        Assertions.assertEquals(String.valueOf(intValue), result);
    }
}

