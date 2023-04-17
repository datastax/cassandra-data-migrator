package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TEXTFormat_InstantCodecTest {

    private TEXTFormat_InstantCodec codec;

    private static final String FORMAT = "yyMMddHHmmss";
    private static final String TIMEZONE = "Europe/Dublin";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
    ZoneOffset zoneOffset = ZoneId.of(TIMEZONE).getRules().getOffset(Instant.now());

    @Mock
    private CqlHelper cqlHelper;

    @BeforeEach
    void setUp() {
        cqlHelper = mock(CqlHelper.class);
        when(cqlHelper.isCodecRegistered(any())).thenReturn(false);

        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, FORMAT);
        sc.set(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, TIMEZONE);
        PropertyHelper propertyHelper = PropertyHelper.getInstance();
        propertyHelper.initializeSparkConf(sc);

        codec = new TEXTFormat_InstantCodec(propertyHelper,cqlHelper);
    }

    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getJavaType_ShouldReturnStringType() {
        Assertions.assertEquals(GenericType.INSTANT, codec.getJavaType());
    }

    @Test
    void getCqlType_ShouldReturnTimestampType() {
        Assertions.assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    void encode_ShouldReturnNull_WhenValueIsNull() {
        ByteBuffer result = codec.encode(null, ProtocolVersion.DEFAULT);
        Assertions.assertNull(result);
    }

    @Test
    void encode_ShouldEncodeInstantToTextByteBuffer_WhenValueIsNotNull() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer expected = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        ByteBuffer result = codec.encode(value, ProtocolVersion.DEFAULT);
        CodecTestHelper.assertByteBufferEquals(expected, result);
    }

    @Test
    void decode_ShouldDecodeTextByteBufferAndReturnAsInstant() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        ByteBuffer byteBuffer = TypeCodecs.TEXT.encode(valueAsString, ProtocolVersion.DEFAULT);

        Instant result = codec.decode(byteBuffer, ProtocolVersion.DEFAULT);
        Assertions.assertEquals(value, result);
    }

    @Test
    void format_ShouldFormatInstantValueAsText() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        String result = codec.format(value);
        Assertions.assertEquals(valueAsString, result);
    }

    @Test
    void parse_ShouldParseTextAndReturnAsInstant() {
        String valueAsString = "220412215715";
        Instant value = LocalDateTime.parse(valueAsString, formatter).toInstant(zoneOffset);
        Instant result = codec.parse(valueAsString);
        Assertions.assertEquals(value,result);
    }
}
