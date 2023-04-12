package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

public class CodecFactory {
    public static TypeCodec<?> getCodec(Codecset codec) {
        switch (codec) {
            case CQL_INT_TO_STRING: return new CqlIntToStringCodec();
            case CQL_BIGINT_TO_STRING: return new CqlBigintToStringCodec();
            case CQL_TIMESTAMP_TO_STRING: return new CqlTimestampToStringCodec();
            default:
                throw new IllegalArgumentException("Unknown codec: " + codec);
        }
    }
}
