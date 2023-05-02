package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.cql.CqlHelper;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class TEXT_BigDecimalCodec extends AbstractBaseCodec<BigDecimal> {

    public TEXT_BigDecimalCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    public @NotNull GenericType<BigDecimal> getJavaType() {
        return GenericType.of(BigDecimal.class);
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(BigDecimal value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = value.toString();
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public BigDecimal decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return new BigDecimal(stringValue);
    }

    @Override
    public @NotNull String format(BigDecimal value) {
        return TypeCodecs.DECIMAL.format(value);
    }

    @Override
    public BigDecimal parse(String value) {
        return value == null ? null : new BigDecimal(value);
    }
}
