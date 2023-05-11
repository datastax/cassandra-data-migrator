package com.datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import static com.datastax.cdm.cql.codec.DOUBLE_StringCodec.DOUBLE_FORMAT;

// This works with decimal-formatted doubles in strings, but not
// with the default scientific notation. A separate codec is needed
// if that is required.
public class TEXT_DoubleCodec extends AbstractBaseCodec<Double> {
    private final DecimalFormat decimalFormat;

    public TEXT_DoubleCodec(PropertyHelper propertyHelper) {
            super(propertyHelper);
            decimalFormat = new DecimalFormat(DOUBLE_FORMAT);
            decimalFormat.setGroupingUsed(false);
            decimalFormat.setRoundingMode(RoundingMode.FLOOR);
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
            String stringValue = decimalFormat.format(value);
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
