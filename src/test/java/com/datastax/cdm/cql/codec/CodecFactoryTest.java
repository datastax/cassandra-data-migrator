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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import com.datastax.cdm.data.MockitoExtension;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.dse.driver.internal.core.type.codec.geometry.LineStringCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PointCodec;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PolygonCodec;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

@ExtendWith(MockitoExtension.class)
class CodecFactoryTest {
    @Mock
    private PropertyHelper propertyHelper;

    @BeforeEach
    void setUp() {
        // Mockito.when(propertyHelper.getString("timestamp.format")).thenReturn("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForPolygonType() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.POLYGON_TYPE);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof PolygonCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForIntString() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.INT_STRING);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof INT_StringCodec);
        assertTrue(codecs.get(1) instanceof TEXT_IntegerCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForDoubleString() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.DOUBLE_STRING);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof DOUBLE_StringCodec);
        assertTrue(codecs.get(1) instanceof TEXT_DoubleCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForBigintString() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.BIGINT_STRING);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof BIGINT_StringCodec);
        assertTrue(codecs.get(1) instanceof TEXT_LongCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForDecimalString() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.DECIMAL_STRING);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof DECIMAL_StringCodec);
        assertTrue(codecs.get(1) instanceof TEXT_BigDecimalCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForTimestampStringMillis() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.TIMESTAMP_STRING_MILLIS);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof TIMESTAMP_StringMillisCodec);
        assertTrue(codecs.get(1) instanceof TEXTMillis_InstantCodec);
    }

    // @Test
    // void getCodecPair_ShouldReturnCorrectCodecsForTimestampStringFormat() {
    // Mockito.when(propertyHelper.getString("timestamp.format")).thenReturn("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    // List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.TIMESTAMP_STRING_FORMAT);
    // assertFalse(codecs.isEmpty());
    // assertTrue(codecs.get(0) instanceof TIMESTAMP_StringFormatCodec);
    // assertTrue(codecs.get(1) instanceof TEXTFormat_InstantCodec);
    // }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForPointType() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.POINT_TYPE);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof PointCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForDateRange() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.DATE_RANGE);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof DateRangeCodec);
    }

    @Test
    void getCodecPair_ShouldReturnCorrectCodecsForLineString() {
        List<TypeCodec<?>> codecs = CodecFactory.getCodecPair(propertyHelper, Codecset.LINE_STRING);
        assertFalse(codecs.isEmpty());
        assertTrue(codecs.get(0) instanceof LineStringCodec);
    }

    @Test
    void getCodecPair_ShouldThrowExceptionForUnknownCodec() {
        assertThrows(NullPointerException.class, () -> {
            CodecFactory.getCodecPair(propertyHelper, null);
        });
    }

}
