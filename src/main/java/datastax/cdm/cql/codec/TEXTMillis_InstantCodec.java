package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * This codec converts a CQL TIMESTAMP to a Java String.
 */
public class TEXTMillis_InstantCodec extends AbstractBaseCodec<Instant> {

    public TEXTMillis_InstantCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        if (cqlHelper.isCodecRegistered(Codecset.TIMESTAMP_STRING_FORMAT))
            throw new RuntimeException("Codec " + Codecset.TIMESTAMP_STRING_FORMAT + " is already registered");
    }

    @Override
    public @NotNull GenericType<Instant> getJavaType() {
        return GenericType.INSTANT;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Instant value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = String.valueOf(value.toEpochMilli());
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Instant decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return Instant.ofEpochMilli(Long.parseLong(stringValue));
    }

    @Override
    public @NotNull String format(Instant value) {
        return String.valueOf(value.toEpochMilli());
    }

    @Override
    public Instant parse(String value) {
        return Instant.ofEpochMilli(Long.parseLong(value));
    }

}

