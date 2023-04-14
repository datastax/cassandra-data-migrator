package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;

public class CodecFactory {
    public static TypeCodec<?> getCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper, Codecset codec) {
        switch (codec) {
            case CQL_INT_TO_STRING: return new CqlIntToStringCodec(propertyHelper, cqlHelper);
            case CQL_BIGINT_TO_STRING: return new CqlBigintToStringCodec(propertyHelper, cqlHelper);
            case CQL_DECIMAL_TO_STRING: return new CqlDecimalToStringCodec(propertyHelper, cqlHelper);
            case CQL_TIMESTAMP_TO_STRING_MILLIS: return new CqlTimestampToString_Millis_Codec(propertyHelper, cqlHelper);
            case CQL_TIMESTAMP_TO_STRING_FORMAT: return new CqlTimestampToString_Format_Codec(propertyHelper, cqlHelper);
            default:
                throw new IllegalArgumentException("Unknown codec: " + codec);
        }
    }
}
