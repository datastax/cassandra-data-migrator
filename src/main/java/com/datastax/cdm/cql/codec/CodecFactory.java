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

import java.util.Arrays;
import java.util.List;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.dse.driver.internal.core.type.codec.geometry.LineStringCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PointCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PolygonCodec;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

public class CodecFactory {
    public static List<TypeCodec<?>> getCodecPair(PropertyHelper propertyHelper, Codecset codec) {
        switch (codec) {
        case INT_STRING:
            return Arrays.asList(new INT_StringCodec(propertyHelper), new TEXT_IntegerCodec(propertyHelper));
        case DOUBLE_STRING:
            return Arrays.asList(new DOUBLE_StringCodec(propertyHelper), new TEXT_DoubleCodec(propertyHelper));
        case BIGINT_STRING:
            return Arrays.asList(new BIGINT_StringCodec(propertyHelper), new TEXT_LongCodec(propertyHelper));
        case BIGINT_BIGINTEGER:
            return Arrays.asList(new BIGINT_BigIntegerCodec(propertyHelper),
                    new BigInteger_BIGINTCodec(propertyHelper));
        case STRING_BLOB:
            return Arrays.asList(new TEXT_BLOBCodec(propertyHelper), new BLOB_TEXTCodec(propertyHelper));
        case ASCII_BLOB:
            return Arrays.asList(new ASCII_BLOBCodec(propertyHelper), new BLOB_ASCIICodec(propertyHelper));
        case DECIMAL_STRING:
            return Arrays.asList(new DECIMAL_StringCodec(propertyHelper), new TEXT_BigDecimalCodec(propertyHelper));
        case TIMESTAMP_STRING_MILLIS:
            return Arrays.asList(new TIMESTAMP_StringMillisCodec(propertyHelper),
                    new TEXTMillis_InstantCodec(propertyHelper));
        case TIMESTAMP_STRING_FORMAT:
            return Arrays.asList(new TIMESTAMP_StringFormatCodec(propertyHelper),
                    new TEXTFormat_InstantCodec(propertyHelper));
        case POLYGON_TYPE:
            return Arrays.asList(new PolygonCodec());
        case POINT_TYPE:
            return Arrays.asList(new PointCodec());
        case DATE_RANGE:
            return Arrays.asList(new DateRangeCodec());
        case LINE_STRING:
            return Arrays.asList(new LineStringCodec());

        default:
            throw new IllegalArgumentException("Unknown codec: " + codec);
        }
    }
}
