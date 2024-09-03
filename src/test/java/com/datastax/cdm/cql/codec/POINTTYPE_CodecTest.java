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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PointCodec;
import com.esri.core.geometry.ogc.OGCPoint;

class POINTTYPE_CodecTest {

    private PointCodec codec;

    @BeforeEach
    void setUp() {
        codec = new PointCodec();
    }

    @Test
    void encode_ShouldEncodePointToByteBuffer() {
        Point point = new DefaultPoint((OGCPoint) OGCPoint.fromText("POINT (30 10)"));
        ByteBuffer encoded = codec.encode(point, CqlConversion.PROTOCOL_VERSION);

        assertNotNull(encoded);
        assertTrue(encoded.remaining() > 0);

        ByteBuffer expected = codec.encode(point, CqlConversion.PROTOCOL_VERSION);
        assertTrue(expected.equals(encoded));
    }

    @Test
    void decode_ShouldDecodeByteBufferToPoint() {
        String wkt = "POINT (30 10)";
        Point expectedPoint = new DefaultPoint((OGCPoint) OGCPoint.fromText(wkt));
        ByteBuffer byteBuffer = codec.encode(expectedPoint, CqlConversion.PROTOCOL_VERSION);

        Point actualPoint = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);

        assertNotNull(actualPoint);
        String actualWkt = actualPoint.asWellKnownText();
        assertEquals(wkt, actualWkt);
    }

    @Test
    void format_ShouldFormatPointToWktString() {
        String wkt = "POINT (30 10)";
        Point point = new DefaultPoint((OGCPoint) OGCPoint.fromText(wkt));

        String formatted = codec.format(point);
        assertNotNull(formatted);

        String unquotedFormatted = formatted.replace("'", "");
        assertEquals(wkt, unquotedFormatted);
    }

    @Test
    void parse_ShouldParseWktStringToPoint() {
        String stringPoint = "POINT (30 10)";
        String quotedPoint = "'" + stringPoint + "'";
        Point parsedPoint = codec.parse(quotedPoint);

        assertNotNull(parsedPoint);
        assertEquals(stringPoint, parsedPoint.asWellKnownText());
    }

    @Test
    void encode_ShouldHandleNullValues() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }

    @Test
    void decode_ShouldHandleNullByteBuffer() {
        Point result = codec.decode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }
}
