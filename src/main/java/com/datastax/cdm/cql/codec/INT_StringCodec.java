package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.cql.CqlHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class INT_StringCodec extends AbstractBaseCodec<String> {

    public INT_StringCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            int intValue = Integer.parseInt(value);
            return TypeCodecs.INT.encode(intValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Integer intValue = TypeCodecs.INT.decode(bytes, protocolVersion);
        return intValue.toString();
    }

    @Override
    public @NotNull String format(String value) {
        int intValue = Integer.parseInt(value);
        return TypeCodecs.INT.format(intValue);
    }

    @Override
    public String parse(String value) {
        Integer intValue = TypeCodecs.INT.parse(value);
        return intValue == null ? null : intValue.toString();
    }
}

