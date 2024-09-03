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

public class TEXT_LongCodec extends AbstractBaseCodec<Long> {

    public TEXT_LongCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<Long> getJavaType() {
        return GenericType.LONG;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Long value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = Long.toString(value);
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Long decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return Long.parseLong(stringValue);
    }

    @Override
    public @NotNull String format(Long value) {
        return TypeCodecs.BIGINT.format(value);
    }

    @Override
    public Long parse(String value) {
        return value == null ? null : Long.parseLong(value);
    }
}
