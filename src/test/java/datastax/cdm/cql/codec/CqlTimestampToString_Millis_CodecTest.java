package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.mockito.Mockito.*;

class CqlTimestampToString_Millis_CodecTest {

    private CqlTimestampToString_Millis_Codec codec;

    @Mock
    private TypeCodec<Instant> timestampCodec;

    @Mock
    private ByteBuffer byteBuffer;

    @Mock
    private CqlHelper cqlHelper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cqlHelper = mock(CqlHelper.class);
        when(cqlHelper.isCodecRegistered(any())).thenReturn(false);
        codec = new CqlTimestampToString_Millis_Codec(null, cqlHelper);
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
    void getCqlType_ShouldReturnTimestampType() {
        Assertions.assertEquals(DataTypes.TIMESTAMP, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeStringValueToByteBuffer_WhenValueIsNotNull() {
        String stringValue = "1681333035000";
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(stringValue));
        byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(instantValue.toEpochMilli());
        byteBuffer.rewind();

        when(timestampCodec.encode(instantValue, ProtocolVersion.DEFAULT)).thenReturn(byteBuffer);
        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(byteBuffer, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToInstantValueAndReturnAsString() {
        String stringValue = "1681333035000";
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(stringValue));
        byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(instantValue.toEpochMilli());
        byteBuffer.rewind();

        when(timestampCodec.decode(byteBuffer, ProtocolVersion.DEFAULT)).thenReturn(instantValue);
        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(stringValue, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsString() {
        String stringValue = "1681333035000";
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(stringValue));

        String formattedValue = codec.format(stringValue);
        Instant formattedInstantValue = TypeCodecs.TIMESTAMP.parse(formattedValue);
        String result = String.valueOf(formattedInstantValue.toEpochMilli());
        Assertions.assertEquals(stringValue, result);
    }

    @Test
    void parse_ShouldParseStringToInstantValueAndReturnAsString() {
        String stringValue = "1681333035000";
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(stringValue));

        when(timestampCodec.parse(stringValue)).thenReturn(instantValue);
        String result = codec.parse(stringValue);
        Assertions.assertEquals(stringValue, result);
    }
}
