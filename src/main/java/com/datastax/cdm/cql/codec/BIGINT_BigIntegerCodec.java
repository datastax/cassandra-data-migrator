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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class BIGINT_BigIntegerCodec extends AbstractBaseCodec<BigInteger> {

    public BIGINT_BigIntegerCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);
    }

    @Override
    public @NotNull GenericType<BigInteger> getJavaType() {
        return GenericType.BIG_INTEGER;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.BIGINT;
    }

    @Override
    public ByteBuffer encode(BigInteger value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            return TypeCodecs.BIGINT.encode(value.longValue(), protocolVersion);
        }
    }

    @Override
    public BigInteger decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        return BigInteger.valueOf(TypeCodecs.BIGINT.decode(bytes, protocolVersion));
    }

    @Override
    public @NotNull String format(BigInteger value) {
        return TypeCodecs.BIGINT.format(value.longValue());
    }

    @Override
    public BigInteger parse(String value) {
        return BigInteger.valueOf(TypeCodecs.BIGINT.parse(value));
    }

}
