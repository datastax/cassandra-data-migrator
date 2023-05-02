package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.cql.CqlHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * This codec converts a CQL INT to a Java String.
 */
public class DOUBLE_StringCodec extends AbstractBaseCodec<String> {

    public DOUBLE_StringCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            double doubleValue = Double.parseDouble(value);
            return TypeCodecs.DOUBLE.encode(doubleValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Double doubleValue = TypeCodecs.DOUBLE.decode(bytes, protocolVersion);
        return doubleValue.toString();
    }

    @Override
    public @NotNull String format(String value) {
        double doubleValue = Double.parseDouble(value);
        return TypeCodecs.DOUBLE.format(doubleValue);
    }

    @Override
    public String parse(String value) {
        Double doubleValue = TypeCodecs.DOUBLE.parse(value);
        return doubleValue == null ? null : doubleValue.toString();
    }
}

