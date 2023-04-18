package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class DECIMAL_StringCodec extends AbstractBaseCodec<String> {

    public DECIMAL_StringCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.DECIMAL;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            BigDecimal decimalValue = new BigDecimal(value);
            return TypeCodecs.DECIMAL.encode(decimalValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        BigDecimal decimalValue = TypeCodecs.DECIMAL.decode(bytes, protocolVersion);
        return decimalValue.toString();
    }

    @Override
    public @NotNull String format(String value) {
        BigDecimal decimalValue = new BigDecimal(value);
        return TypeCodecs.DECIMAL.format(decimalValue);
    }

    @Override
    public String parse(String value) {
        BigDecimal decimalValue = TypeCodecs.DECIMAL.parse(value);
        return decimalValue == null ? null : decimalValue.toString();
    }
}

