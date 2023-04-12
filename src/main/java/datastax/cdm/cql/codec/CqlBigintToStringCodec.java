package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * This codec converts a CQL BigInt to a Java String.
 */
public class CqlBigintToStringCodec implements TypeCodec<String> {

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.BIGINT;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            long longValue = Long.parseLong(value);
            return TypeCodecs.BIGINT.encode(longValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Long longValue = TypeCodecs.BIGINT.decode(bytes, protocolVersion);
        return longValue.toString();
    }

    @Override
    public @NotNull String format(String value) {
        long longValue = Long.parseLong(value);
        return TypeCodecs.BIGINT.format(longValue);
    }

    @Override
    public String parse(String value) {
        Long longValue = TypeCodecs.BIGINT.parse(value);
        return longValue == null ? null : longValue.toString();
    }
}

