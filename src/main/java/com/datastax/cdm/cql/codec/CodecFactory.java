package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.dse.driver.internal.core.type.codec.geometry.LineStringCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PointCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PolygonCodec;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

import java.util.Arrays;
import java.util.List;

public class CodecFactory {
    public static List<TypeCodec<?>> getCodecPair(PropertyHelper propertyHelper, Codecset codec) {
        switch (codec) {
            case INT_STRING: return Arrays.asList(new INT_StringCodec(propertyHelper), new TEXT_IntegerCodec(propertyHelper));
            case DOUBLE_STRING: return Arrays.asList(new DOUBLE_StringCodec(propertyHelper), new TEXT_DoubleCodec(propertyHelper));
            case BIGINT_STRING: return Arrays.asList(new BIGINT_StringCodec(propertyHelper), new TEXT_LongCodec(propertyHelper));
            case DECIMAL_STRING: return Arrays.asList(new DECIMAL_StringCodec(propertyHelper), new TEXT_BigDecimalCodec(propertyHelper));
            case TIMESTAMP_STRING_MILLIS: return Arrays.asList(new TIMESTAMP_StringMillisCodec(propertyHelper), new TEXTMillis_InstantCodec(propertyHelper));
            case TIMESTAMP_STRING_FORMAT: return Arrays.asList(new TIMESTAMP_StringFormatCodec(propertyHelper), new TEXTFormat_InstantCodec(propertyHelper));
            case POLYGON_TYPE: return Arrays.asList(new PolygonCodec());
            case POINT_TYPE: return Arrays.asList(new PointCodec());
            case DATE_RANGE: return Arrays.asList(new DateRangeCodec());
            case LINE_STRING: return Arrays.asList(new LineStringCodec());

            default:
                throw new IllegalArgumentException("Unknown codec: " + codec);
        }
    }
}
