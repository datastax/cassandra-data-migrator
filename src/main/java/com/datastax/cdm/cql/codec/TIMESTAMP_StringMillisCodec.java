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
import java.time.Instant;

import org.jetbrains.annotations.NotNull;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class TIMESTAMP_StringMillisCodec extends AbstractBaseCodec<String> {

    public TIMESTAMP_StringMillisCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            Instant instantValue = Instant.ofEpochMilli(Long.parseLong(value));
            return TypeCodecs.TIMESTAMP.encode(instantValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Instant instantValue = TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion);
        return instantValue == null ? null : String.valueOf(instantValue.toEpochMilli());
    }

    @Override
    // We get in a string of our format, we need to convert to a proper CQL-formatted string
    public @NotNull String format(String value) {
        Instant instantValue = Instant.ofEpochMilli(Long.parseLong(value));
        return TypeCodecs.TIMESTAMP.format(instantValue);
    }

    @Override
    // We get in a proper CQL-formatted string, we need to convert to our format
    public String parse(String value) {
        Instant instantValue = TypeCodecs.TIMESTAMP.parse(value);
        return instantValue == null ? null : String.valueOf(instantValue.toEpochMilli());
    }
}
