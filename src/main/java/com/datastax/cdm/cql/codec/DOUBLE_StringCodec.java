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
public class DOUBLE_StringCodec extends AbstractBaseCodec<String> {

    // TODO: this could be made configurable as TIMESTAMP_StringFormatCodec
    public static final String DOUBLE_FORMAT = "0.#########";

    private final DecimalFormat decimalFormat;

    public DOUBLE_StringCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
        decimalFormat = new DecimalFormat(DOUBLE_FORMAT);
        decimalFormat.setGroupingUsed(false);
        decimalFormat.setRoundingMode(RoundingMode.FLOOR);
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
        return doubleValue == null ? null : decimalFormat.format(doubleValue);
    }

    @Override
    public @NotNull String format(String value) {
        double doubleValue = Double.parseDouble(value);
        return decimalFormat.format(doubleValue);
    }

    @Override
    public String parse(String value) {
        Double doubleValue = TypeCodecs.DOUBLE.parse(value);
        return doubleValue == null ? null : decimalFormat.format(doubleValue);
    }
}
