package com.datastax.cdm.cql.codec;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.type.codec.geometry.LineStringCodec;
import com.esri.core.geometry.ogc.OGCLineString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class LINESTRINGTYPE_CodecTest {

    private LineStringCodec codec;

    @BeforeEach
    void setUp() {
        codec = new LineStringCodec();
    }

    @Test
    void encode_ShouldEncodeLineStringToByteBuffer() {
        LineString lineString = new DefaultLineString((OGCLineString) OGCLineString.fromText("LINESTRING (30 10, 10 30, 40 40)"));
        ByteBuffer encoded = codec.encode(lineString, CqlConversion.PROTOCOL_VERSION);

        assertNotNull(encoded);
        assertTrue(encoded.remaining() > 0);

        ByteBuffer expected = codec.encode(lineString, CqlConversion.PROTOCOL_VERSION);
        assertTrue(expected.equals(encoded));
    }

    @Test
    void decode_ShouldDecodeByteBufferToLineString() {
        String lineString = "LINESTRING (30 10, 10 30, 40 40)";
        LineString expectedLineString = new DefaultLineString((OGCLineString) OGCLineString.fromText(lineString));
        ByteBuffer byteBuffer = codec.encode(expectedLineString, CqlConversion.PROTOCOL_VERSION);

        LineString actualLineString = codec.decode(byteBuffer, CqlConversion.PROTOCOL_VERSION);

        assertNotNull(actualLineString);
        String actualWkt = actualLineString.asWellKnownText();
        assertEquals(lineString, actualWkt);
    }

    @Test
    void format_ShouldFormatLineStringToWktString() {
        String line = "LINESTRING (30 10, 10 30, 40 40)";
        LineString lineString = new DefaultLineString((OGCLineString) OGCLineString.fromText(line));

        String formatted = codec.format(lineString);
        assertNotNull(formatted);

        String unquotedFormatted = formatted.replace("'", "");
        assertEquals(line, unquotedFormatted);
    }

    @Test
    void parse_ShouldParseWktStringToLineString() {
        String stringLineString = "LINESTRING (30 10, 10 30, 40 40)";
        String quotedLineString = "'" + stringLineString + "'";
        LineString parsedLineString = codec.parse(quotedLineString);

        assertNotNull(parsedLineString);
        assertEquals(stringLineString, parsedLineString.asWellKnownText());
    }

    @Test
    void encode_ShouldHandleNullValues() {
        ByteBuffer result = codec.encode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }

    @Test
    void decode_ShouldHandleNullByteBuffer() {
        LineString result = codec.decode(null, CqlConversion.PROTOCOL_VERSION);
        assertNull(result);
    }
}
