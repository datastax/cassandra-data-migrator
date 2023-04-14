package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CqlTimestampToString_yyMMddHHmmss_CodecTest {

    private CqlTimestampToString_Format_Codec codec;

    private static final String FORMAT = "yyMMddHHmmss";
    private static final String TIMEZONE = "Europe/Dublin";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
    ZoneOffset zoneOffset = ZoneId.of(TIMEZONE).getRules().getOffset(Instant.now());

    @Mock
    private CqlHelper cqlHelper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cqlHelper = mock(CqlHelper.class);
        when(cqlHelper.isCodecRegistered(any())).thenReturn(false);

        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        codec = new CqlTimestampToString_Format_Codec(propertyHelper,cqlHelper);
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
        String stringValue = "220412215715";
        LocalDateTime localDateTime = LocalDateTime.parse(stringValue, formatter);
        Instant instantValue = localDateTime.toInstant(zoneOffset);

        ByteBuffer expectedByteBuffer = ByteBuffer.allocate(8);
        expectedByteBuffer.putLong(instantValue.toEpochMilli());
        expectedByteBuffer.rewind();

        ByteBuffer result = codec.encode(stringValue, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(expectedByteBuffer, result);
    }

    @Test
    void decode_ShouldDecodeByteBufferToInstantValueAndReturnAsString() {
        String stringValue = "220412215715";
        LocalDateTime localDateTime = LocalDateTime.parse(stringValue, formatter);
        Instant instantValue = localDateTime.toInstant(zoneOffset);

        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(instantValue.toEpochMilli());
        byteBuffer.rewind();

        String result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(stringValue, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsString() {
        String stringValue = "220412215715";
        LocalDateTime localDateTime = LocalDateTime.parse(stringValue, formatter);
        Instant instantValue = localDateTime.toInstant(zoneOffset);

        String result = codec.format(stringValue);
        Assertions.assertEquals(stringValue, result);
    }

    @Test
    void parse_ShouldParseStringToInstantValueAndReturnAsString() {
        String stringValue = "220412215715";
        LocalDateTime localDateTime = LocalDateTime.parse(stringValue, formatter);
        Instant instantValue = localDateTime.toInstant(zoneOffset);

        String result = codec.parse(stringValue);
        Assertions.assertEquals(String.valueOf(instantValue.toEpochMilli()), result);
    }
}
