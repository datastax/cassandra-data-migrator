/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.cql.codec;

import static com.datastax.cdm.cql.codec.DOUBLE_StringCodec.DOUBLE_FORMAT;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import org.jetbrains.annotations.NotNull;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

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
        return Double.valueOf(stringValue);
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
