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

public class TIMESTAMP_StringMillisCodec extends AbstractBaseCodec<String> {

    public TIMESTAMP_StringMillisCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        if (cqlHelper.isCodecRegistered(Codecset.TIMESTAMP_STRING_FORMAT))
            throw new RuntimeException("Codec " + Codecset.TIMESTAMP_STRING_FORMAT + " is already registered");
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
    // We get in a string of our format, we need to convert to a proper CQL-formatted string
    public @NotNull String format(String value) {
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(value));
        return TypeCodecs.TIMESTAMP.format(instantValue);
    }

    @Override
    // We get in a proper CQL-formatted string, we need to convert to our format
    public String parse(String value) {
        Instant instantValue = TypeCodecs.TIMESTAMP.parse(value);
        return instantValue == null ? null : String.valueOf(instantValue.toEpochMilli());
    }
}
