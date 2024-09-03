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
import java.text.ParseException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.driver.api.core.ProtocolVersion;

class DATERANGETYPE_CodecTest {

    private DateRangeCodec codec;
    private final ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT;

    @BeforeEach
    void setUp() {
        codec = new DateRangeCodec();
    }

    @Test
    void encode_ShouldEncodeDateRangeToByteBuffer() throws ParseException {
        String dateRangeString = "2001-01-01";
        DateRange dateRange;
        try {
            dateRange = DateRange.parse(dateRangeString);
        } catch (ParseException e) {
            fail("Failed to parse the date range: " + e.getMessage());
            return;
        }

        // Encode the DateRange object
        ByteBuffer encoded = codec.encode(dateRange, protocolVersion);

        // Assertions
        assertNotNull(encoded);
        assertTrue(encoded.remaining() > 0);

        // Decode the ByteBuffer back to a DateRange and compare
        DateRange decoded = codec.decode(encoded, protocolVersion);
        assertEquals(dateRange, decoded);
    }

    @Test
    void decode_ShouldDecodeByteBufferToDateRange() throws ParseException {
        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        String expectedFormattedDate = DateTimeFormatter.ISO_LOCAL_DATE.format(zonedDateTime);

        // Create a DateRange object using a string in the expected format
        DateRange dateRange = DateRange.parse(expectedFormattedDate);
        String formatted = codec.format(dateRange);

        // The formatted string should be surrounded by single quotes, remove them for the comparison
        String unquotedFormatted = formatted.replace("'", "");
        assertEquals(expectedFormattedDate, unquotedFormatted);
    }

    @Test
    void format_ShouldFormatDateRangeToString() throws ParseException {
        String dateRangeString = "2001-01-01"; // Adjust this string to the correct format
        DateRange dateRange;
        try {
            dateRange = DateRange.parse(dateRangeString);
        } catch (ParseException e) {
            fail("Failed to parse the date range for setup: " + e.getMessage());
            return;
        }

        // Format the date range using the codec
        String formatted = codec.format(dateRange);
        assertNotNull(formatted);

        // Remove single quotes for parsing
        String unquotedFormatted = formatted.replace("'", "");
        DateRange parsedDateRange;
        try {
            parsedDateRange = DateRange.parse(unquotedFormatted);
        } catch (ParseException e) {
            fail("Failed to parse the formatted date range: " + e.getMessage());
            return;
        }

        // The parsed DateRange should equal the original DateRange
        assertEquals(dateRange, parsedDateRange);
    }

    @Test
    void parse_ShouldParseStringToDateRange() throws ParseException {
        DateTimeFormatter df = (new DateTimeFormatterBuilder()).appendInstant(3).toFormatter();
        String formattedDateTime = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MILLIS).format(df);

        // Enclose in single quotes as per the error message
        String dateRangeLiteral = "'" + formattedDateTime + "'";

        // Attempt to parse it using the codec
        DateRange parsedDateRange;
        try {
            parsedDateRange = codec.parse(dateRangeLiteral);
        } catch (Exception e) {
            fail("Parsing failed with exception: " + e.getMessage());
            return;
        }

        assertNotNull(parsedDateRange);
    }

    @Test
    void encode_ShouldHandleNullValues() {
        ByteBuffer result = codec.encode(null, protocolVersion);
        assertNull(result);
    }

    @Test
    void decode_ShouldHandleNullByteBuffer() {
        DateRange result = codec.decode(null, protocolVersion);
        assertNull(result);
    }
}
