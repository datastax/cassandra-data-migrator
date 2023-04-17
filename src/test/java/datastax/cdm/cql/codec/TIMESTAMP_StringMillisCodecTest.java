package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
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
import java.time.LocalDateTime;

import static org.mockito.Mockito.*;

class TIMESTAMP_StringMillisCodecTest {

    private TIMESTAMP_StringMillisCodec codec;

    @Mock
    private CqlHelper cqlHelper;

    @BeforeEach
    void setUp() {
        cqlHelper = mock(CqlHelper.class);
        when(cqlHelper.isCodecRegistered(any())).thenReturn(false);
        codec = new TIMESTAMP_StringMillisCodec(null, cqlHelper);
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
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer expected = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(valueAsString, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToValueAndReturnAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        ByteBuffer byteBuffer = TypeCodecs.TIMESTAMP.encode(value, ProtocolVersion.DEFAULT);

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void format_ShouldFormatValueAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String expected = TypeCodecs.TIMESTAMP.format(value);

        String result = codec.format(valueAsString);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void parse_ShouldParseStringToInstantValueAndReturnAsString() {
        String valueAsString = "1681333035000";
        Instant value = Instant.ofEpochMilli(Long.parseLong(valueAsString));
        String result = codec.parse(TypeCodecs.TIMESTAMP.format(value));
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldThrowIllegalArgumentException_WhenValueIsNotANumber() {
        String valueAsString = "not a millis timestamp";
        Assertions.assertThrows(IllegalArgumentException.class, () -> codec.parse(valueAsString));
    }
}
