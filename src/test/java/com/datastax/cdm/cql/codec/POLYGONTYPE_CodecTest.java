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
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.dse.driver.internal.core.type.codec.geometry.PolygonCodec;
import com.esri.core.geometry.ogc.OGCPolygon;

class POLYGONTYPE_CodecTest {

    private PolygonCodec codec;

    @BeforeEach
    void setUp() {
        codec = new PolygonCodec();
    }

    @Test
    void encode_ShouldEncodePolygonToByteBuffer() {
        Polygon polygon = new DefaultPolygon(
                (OGCPolygon) OGCPolygon.fromText("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"));
        ByteBuffer encoded = codec.encode(polygon, CqlConversion.PROTOCOL_VERSION); // Assuming protocol version is not
                                                                                    // needed or a mock version is
                                                                                    // provided

        // Assert that the result is not null
        assertNotNull(encoded);

        // Assert that the ByteBuffer is not empty
        assertTrue(encoded.remaining() > 0);

        ByteBuffer expected = codec.encode(polygon, CqlConversion.PROTOCOL_VERSION);
        assertTrue(expected.equals(encoded));
    }

    @Test
    void decode_ShouldDecodeByteBufferToPolygon() {
        // Create a ByteBuffer that represents a Polygon
        String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        Polygon expectedPolygon = new DefaultPolygon((OGCPolygon) OGCPolygon.fromText(wkt));
        ByteBuffer byteBuffer = codec.encode(expectedPolygon, CqlConversion.PROTOCOL_VERSION);

        // Use the codec to decode this ByteBuffer into a Polygon object
        Polygon actualPolygon = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);

        // Assert that the result is not null
        assertNotNull(actualPolygon);

        // Assert that the actual Polygon object is equal to the expected Polygon object
        String actualWkt = actualPolygon.asWellKnownText();
        assertEquals(wkt, actualWkt);
    }

    @Test
    void format_ShouldFormatPolygonToWktString() {
        String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        Polygon polygon = new DefaultPolygon((OGCPolygon) OGCPolygon.fromText(wkt));

        String formatted = codec.format(polygon);
        assertNotNull(formatted);

        String unquotedFormatted = formatted.replace("'", "");
        assertEquals(wkt, unquotedFormatted);
    }

    @Test
    void parse_ShouldParseWktStringToPolygon() {
        String stringPolygon = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        String quotedPolygon = "'" + stringPolygon + "'";
        // Parse it using the codec
        Polygon parsedPolygon = codec.parse(quotedPolygon);

        // Assert that the resulting Polygon object is as expected
        assertNotNull(parsedPolygon);
        assertEquals(stringPolygon, parsedPolygon.asWellKnownText());
    }

    @Test
    void encode_ShouldHandleNullValues() {
        // Call encode with a null Polygon
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }

    @Test
    void decode_ShouldHandleNullByteBuffer() {
        // Call decode with a null ByteBuffer
        Polygon result = codec.decode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }
}
