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

public class BLOB_TEXTCodec extends AbstractBaseCodec<String> {

    public BLOB_TEXTCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.BLOB;
    }

    @Override
    public ByteBuffer encode(String value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            return TypeCodecs.BLOB.encode(ByteBuffer.wrap(value.getBytes()), protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        return TypeCodecs.TEXT.decode(bytes, protocolVersion);
    }

    @Override
    public @NotNull String format(String value) {
        ByteBuffer bb = ByteBuffer.wrap(value.getBytes());
        return TypeCodecs.BLOB.format(bb);
    }

    @Override
    public String parse(String value) {
        ByteBuffer bb = TypeCodecs.BLOB.parse(value);
        return bb == null ? null : bb.toString();
    }
}
