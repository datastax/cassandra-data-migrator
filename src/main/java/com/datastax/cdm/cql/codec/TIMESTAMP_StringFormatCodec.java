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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRulesProvider;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * This codec converts a TIMESTAMP to a Java String with format specified at
 * KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT using the zone specified at
 * KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE.
 */
public class TIMESTAMP_StringFormatCodec extends AbstractBaseCodec<String> {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final DateTimeFormatter formatter;
    private final ZoneOffset zoneOffset;

    public TIMESTAMP_StringFormatCodec(PropertyHelper propertyHelper) {
        super(propertyHelper);

        String formatString = propertyHelper.getString(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT);
        if (formatString == null || formatString.isEmpty()) {
            throw new IllegalArgumentException("Property " + KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT
                    + " is required and cannot be empty.");
        }
        this.formatter = DateTimeFormatter.ofPattern(formatString);

        String zone = propertyHelper.getString(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE);
        if (zone == null || !ZoneRulesProvider.getAvailableZoneIds().contains(zone)) {
            throw new IllegalArgumentException(
                    "Property " + KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE
                            + " is required and must be a valid ZoneOffset.");
        }
        this.zoneOffset = ZoneId.of(zone).getRules().getOffset(Instant.now());
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
        }
        Instant instantValue = LocalDateTime.parse(value, formatter).toInstant(zoneOffset);
        return TypeCodecs.TIMESTAMP.encode(instantValue, protocolVersion);
    }

    @Override
    public String decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        Instant instantValue = TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instantValue, zoneOffset);
        return formatter.format(localDateTime);
    }

    @Override
    // We get in a string of our format, we need to convert to a proper CQL-formatted string
    public @NotNull String format(String value) {
        Instant instantValue = LocalDateTime.parse(value, formatter).toInstant(zoneOffset);
        return TypeCodecs.TIMESTAMP.format(instantValue);
    }

    @Override
    // We get in a proper CQL-formatted string, we need to convert to our format
    public String parse(String value) {
        Instant instantValue = TypeCodecs.TIMESTAMP.parse(value);
        return formatter.format(instantValue.atOffset(zoneOffset));
    }
}
