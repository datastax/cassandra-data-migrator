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

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class BIGINT_StringCodec extends AbstractBaseCodec<String> {

    public BIGINT_StringCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.BIGINT;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            long longValue = Long.parseLong(value);
            return TypeCodecs.BIGINT.encode(longValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Long longValue = TypeCodecs.BIGINT.decode(bytes, protocolVersion);
        return longValue.toString();
    }

    @Override
    public @NotNull String format(String value) {
        long longValue = Long.parseLong(value);
        return TypeCodecs.BIGINT.format(longValue);
    }

    @Override
    public String parse(String value) {
        Long longValue = TypeCodecs.BIGINT.parse(value);
        return longValue == null ? null : longValue.toString();
    }

}
