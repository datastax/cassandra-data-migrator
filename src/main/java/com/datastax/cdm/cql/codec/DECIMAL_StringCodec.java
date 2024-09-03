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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class DECIMAL_StringCodec extends AbstractBaseCodec<String> {

    public DECIMAL_StringCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
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
