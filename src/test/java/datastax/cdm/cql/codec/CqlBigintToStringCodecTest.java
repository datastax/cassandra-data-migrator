package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.properties.PropertyHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.when;

// This test class brought to you by ChatGPT...
class CqlBigintToStringCodecTest {

    private CqlBigintToStringCodec codec;

    @Mock
    private TypeCodec<Long> longCodec;

    @Mock
    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        codec = new CqlBigintToStringCodec(null,null);
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
    void encode_ShouldEncodeIntValueToByteBuffer_WhenValueIsNotNull() {
        String stringValue = "9223372036854775807";
        long longValue = Long.parseLong(stringValue);
        byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(longValue);
        byteBuffer.rewind();

        when(longCodec.encode(longValue, ProtocolVersion.DEFAULT)).thenReturn(byteBuffer);
        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(byteBuffer, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToIntValueAndReturnAsString() {
        long longValue = 9223372036854775807L;
        byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(longValue);
        byteBuffer.rewind();

        when(longCodec.decode(byteBuffer, ProtocolVersion.DEFAULT)).thenReturn(longValue);
        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(String.valueOf(longValue), result);
    }

    @Test
    void format_ShouldFormatIntValueAsString() {
        long longValue = 9223372036854775807L;
        when(longCodec.format(longValue)).thenReturn(String.valueOf(longValue));
        String result = codec.format(String.valueOf(longValue));
        Assertions.assertEquals(String.valueOf(longValue), result);
    }

    @Test
    void parse_ShouldParseStringToIntValueAndReturnAsString() {
        long longValue = 9223372036854775807L;
        when(longCodec.parse(String.valueOf(longValue))).thenReturn(longValue);
        String result = codec.parse(String.valueOf(longValue));
        Assertions.assertEquals(String.valueOf(longValue), result);
    }
}

