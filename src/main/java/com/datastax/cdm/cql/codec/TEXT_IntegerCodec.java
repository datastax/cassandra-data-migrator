package com.datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class TEXT_IntegerCodec extends AbstractBaseCodec<Integer> {

    public TEXT_IntegerCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<Integer> getJavaType() {
        return GenericType.INTEGER;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Integer value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = value.toString();
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Integer decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return Integer.parseInt(stringValue);
    }

    @Override
    public @NotNull String format(Integer value) {
        return TypeCodecs.INT.format(value);
    }

    @Override
    public Integer parse(String value) {
        return value == null ? null : Integer.parseInt(value);
    }
}
