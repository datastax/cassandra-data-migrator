package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * This codec converts a CQL TIMESTAMP to a Java String.
 */
public class CqlTimestampToString_Millis_Codec extends AbstractBaseCodec<String> {

    public CqlTimestampToString_Millis_Codec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        if (cqlHelper.isCodecRegistered(Codecset.CQL_TIMESTAMP_TO_STRING_FORMAT))
            throw new RuntimeException("Codec " + Codecset.CQL_TIMESTAMP_TO_STRING_FORMAT + " is already registered");
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            Instant instantValue = Instant.ofEpochMilli(Long.parseLong(value));
            return TypeCodecs.TIMESTAMP.encode(instantValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Instant instantValue = TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion);
        return instantValue == null ? null : String.valueOf(instantValue.toEpochMilli());
    }

    @Override
    public @NotNull String format(String value) {
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(value));
        return TypeCodecs.TIMESTAMP.format(instantValue);
    }

    @Override
    public String parse(String value) {
        Instant instantValue = TypeCodecs.TIMESTAMP.parse(value);
        return instantValue == null ? null : String.valueOf(instantValue.toEpochMilli());
    }
}
