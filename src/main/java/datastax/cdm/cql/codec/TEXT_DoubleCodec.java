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

public class TEXT_DoubleCodec extends AbstractBaseCodec<Double> {

    public TEXT_DoubleCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    public @NotNull GenericType<Double> getJavaType() {
        return GenericType.DOUBLE;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Double value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = value.toString();
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Double decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return new Double(stringValue);
    }

    @Override
    public @NotNull String format(Double value) {
        return TypeCodecs.DOUBLE.format(value);
    }

    @Override
    public Double parse(String value) {
        return value == null ? null : Double.parseDouble(value);
    }

}
