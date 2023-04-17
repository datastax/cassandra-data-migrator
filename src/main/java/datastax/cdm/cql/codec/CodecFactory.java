package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;

import java.util.Arrays;
import java.util.List;

public class CodecFactory {
    public static List<TypeCodec<?>> getCodecs(PropertyHelper propertyHelper, CqlHelper cqlHelper, Codecset codec) {
        switch (codec) {
            case INT_STRING: return Arrays.asList(new INT_StringCodec(propertyHelper, cqlHelper), new TEXT_IntegerCodec(propertyHelper, cqlHelper));
            case DOUBLE_STRING: return Arrays.asList(new DOUBLE_StringCodec(propertyHelper, cqlHelper), new TEXT_DoubleCodec(propertyHelper, cqlHelper));
            case BIGINT_STRING: return Arrays.asList(new BIGINT_StringCodec(propertyHelper, cqlHelper), new TEXT_LongCodec(propertyHelper, cqlHelper));
            case DECIMAL_STRING: return Arrays.asList(new DECIMAL_StringCodec(propertyHelper, cqlHelper), new TEXT_BigDecimalCodec(propertyHelper, cqlHelper));
            case TIMESTAMP_STRING_MILLIS: return Arrays.asList(new TIMESTAMP_StringMillisCodec(propertyHelper, cqlHelper), new TEXTMillis_InstantCodec(propertyHelper, cqlHelper));
            case TIMESTAMP_STRING_FORMAT: return Arrays.asList(new TIMESTAMP_StringFormatCodec(propertyHelper, cqlHelper), new TEXTFormat_InstantCodec(propertyHelper, cqlHelper));
            default:
                throw new IllegalArgumentException("Unknown codec: " + codec);
        }
    }
}
