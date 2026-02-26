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
package com.datastax.cdm.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

@ExtendWith(MockitoExtension.class)
class CqlConversionTest {

    private DataType fromDataType;
    private DataType toDataType;
    private CodecRegistry codecRegistry;

    @BeforeEach
    void setUp() {
        fromDataType = mock(DataType.class);
        toDataType = mock(DataType.class);
        codecRegistry = mock(CodecRegistry.class);
    }

    @Test
    void testConstructorThrowsIllegalArgumentExceptionWhenArgumentsAreNull() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new CqlConversion(null, toDataType, codecRegistry), "null fromDataType"),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new CqlConversion(fromDataType, null, codecRegistry), "null toDataType"),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new CqlConversion(fromDataType, toDataType, null), "null codecRegistry"));
    }

    @Test
    void testConvertWhenConversionTypeIsUnsupported() {
        CqlConversion.Type conversionType = CqlConversion.Type.NONE;
        List<CqlConversion.Type> conversionTypeList = Collections.singletonList(conversionType);
        CqlConversion cqlConversion = new CqlConversion(DataTypes.INT, DataTypes.setOf(DataTypes.INT), codecRegistry);

        Object inputData = new Object();
        Object result = cqlConversion.convert(inputData);

        assertSame(inputData, result);
        assertTrue(cqlConversion.getFromDataTypeList().contains(DataTypes.INT));
        assertTrue(cqlConversion.getToDataTypeList().contains(DataTypes.setOf(DataTypes.INT)));
        assertTrue(cqlConversion.getConversionTypeList().contains(CqlConversion.Type.UNSUPPORTED));
    }

    @Test
    void testConvertWhenConversionTypeIsCollection() {
        TypeCodec tc = mock(TypeCodec.class);
        when(codecRegistry.codecFor(any(DataType.class))).thenReturn(tc);
        when(tc.getJavaType()).thenReturn(GenericType.INTEGER);
        CqlConversion cqlConversion = new CqlConversion(DataTypes.setOf(DataTypes.INT),
                DataTypes.setOf(DataTypes.BIGINT), codecRegistry);

        Object inputData = new Object();
        Object result = cqlConversion.convert(inputData);

        assertSame(inputData, result);
        assertTrue(cqlConversion.getFromDataTypeList().contains(DataTypes.INT));
        assertTrue(cqlConversion.getToDataTypeList().contains(DataTypes.BIGINT));
    }

    @Test
    void testGetConversions() {
        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> CqlConversion.getConversions(null, null),
                "Both types null"));
    }

    // ========== Collection Type Conversion Tests ==========

    @Test
    void testConvertListOfIntToListOfBigint() {
        // Setup codecs for INT and BIGINT
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(eq(DataTypes.INT), eq(Integer.class))).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(eq(DataTypes.BIGINT), eq(Long.class))).thenReturn((TypeCodec) bigintCodec);
        when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(DataTypes.BIGINT)).thenReturn((TypeCodec) bigintCodec);

        when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);
        when(bigintCodec.getJavaType()).thenReturn(GenericType.LONG);

        // Mock encode/decode behavior
        when(intCodec.encode(anyInt(), any())).thenAnswer(invocation -> {
            Integer value = invocation.getArgument(0);
            return ByteBuffer.allocate(4).putInt(value).flip();
        });
        when(bigintCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            return (long) buffer.getInt();
        });

        CqlConversion conversion = new CqlConversion(DataTypes.listOf(DataTypes.INT),
                DataTypes.listOf(DataTypes.BIGINT), codecRegistry);

        List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
        Object result = conversion.convert(inputList);

        assertNotNull(result);
        assertTrue(result instanceof List);
        List<?> resultList = (List<?>) result;
        assertEquals(5, resultList.size());
        assertEquals(CqlConversion.Type.LIST, conversion.getConversionTypeList().get(0));
    }

    @Test
    void testConvertSetOfFloatToSetOfDouble() {
        // Setup codecs for FLOAT and DOUBLE
        @SuppressWarnings("unchecked")
        TypeCodec<Float> floatCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Double> doubleCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(eq(DataTypes.FLOAT), eq(Float.class))).thenReturn((TypeCodec) floatCodec);
        when(codecRegistry.codecFor(eq(DataTypes.DOUBLE), eq(Double.class))).thenReturn((TypeCodec) doubleCodec);
        when(codecRegistry.codecFor(DataTypes.FLOAT)).thenReturn((TypeCodec) floatCodec);
        when(codecRegistry.codecFor(DataTypes.DOUBLE)).thenReturn((TypeCodec) doubleCodec);

        when(floatCodec.getJavaType()).thenReturn(GenericType.FLOAT);
        when(doubleCodec.getJavaType()).thenReturn(GenericType.DOUBLE);

        // Mock encode/decode behavior
        when(floatCodec.encode(anyFloat(), any())).thenAnswer(invocation -> {
            Float value = invocation.getArgument(0);
            return ByteBuffer.allocate(4).putFloat(value).flip();
        });
        when(doubleCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            return (double) buffer.getFloat();
        });

        CqlConversion conversion = new CqlConversion(DataTypes.setOf(DataTypes.FLOAT),
                DataTypes.setOf(DataTypes.DOUBLE), codecRegistry);

        Set<Float> inputSet = new HashSet<>(Arrays.asList(1.1f, 2.2f, 3.3f));
        Object result = conversion.convert(inputSet);

        assertNotNull(result);
        assertTrue(result instanceof Set);
        Set<?> resultSet = (Set<?>) result;
        assertEquals(3, resultSet.size());
        assertEquals(CqlConversion.Type.SET, conversion.getConversionTypeList().get(0));
    }

    @Test
    void testConvertMapOfIntToTextToMapOfBigintToText() {
        // Setup codecs
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<String> textCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(eq(DataTypes.INT), eq(Integer.class))).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(eq(DataTypes.BIGINT), eq(Long.class))).thenReturn((TypeCodec) bigintCodec);
        when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(DataTypes.BIGINT)).thenReturn((TypeCodec) bigintCodec);
        lenient().when(codecRegistry.codecFor(DataTypes.TEXT)).thenReturn((TypeCodec) textCodec);

        when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);
        when(bigintCodec.getJavaType()).thenReturn(GenericType.LONG);
        lenient().when(textCodec.getJavaType()).thenReturn(GenericType.STRING);

        // Mock encode/decode behavior for keys
        when(intCodec.encode(anyInt(), any())).thenAnswer(invocation -> {
            Integer value = invocation.getArgument(0);
            return ByteBuffer.allocate(4).putInt(value).flip();
        });
        when(bigintCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            return (long) buffer.getInt();
        });

        CqlConversion conversion = new CqlConversion(DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT),
                DataTypes.mapOf(DataTypes.BIGINT, DataTypes.TEXT), codecRegistry);

        Map<Integer, String> inputMap = new HashMap<>();
        inputMap.put(1, "one");
        inputMap.put(2, "two");
        inputMap.put(3, "three");

        Object result = conversion.convert(inputMap);

        assertNotNull(result);
        assertTrue(result instanceof Map);
        Map<?, ?> resultMap = (Map<?, ?>) result;
        assertEquals(3, resultMap.size());
        assertEquals(CqlConversion.Type.MAP, conversion.getConversionTypeList().get(0));
    }

    @Test
    void testConvertListWithNoConversionNeeded() {
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);

        lenient().when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        lenient().when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);

        CqlConversion conversion = new CqlConversion(DataTypes.listOf(DataTypes.INT), DataTypes.listOf(DataTypes.INT),
                codecRegistry);

        List<Integer> inputList = Arrays.asList(1, 2, 3);
        Object result = conversion.convert(inputList);

        // Should return the same object when no conversion is needed
        assertSame(inputList, result);
        assertEquals(CqlConversion.Type.LIST, conversion.getConversionTypeList().get(0));
        assertEquals(CqlConversion.Type.NONE, conversion.getConversionTypeList().get(1));
    }

    @Test
    void testConvertNullCollection() {
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(DataTypes.BIGINT)).thenReturn((TypeCodec) bigintCodec);
        when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);
        when(bigintCodec.getJavaType()).thenReturn(GenericType.LONG);

        CqlConversion conversion = new CqlConversion(DataTypes.listOf(DataTypes.INT),
                DataTypes.listOf(DataTypes.BIGINT), codecRegistry);

        Object result = conversion.convert(null);
        assertNull(result);
    }

    @Test
    void testConvertEmptyList() {
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(DataTypes.BIGINT)).thenReturn((TypeCodec) bigintCodec);
        when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);
        when(bigintCodec.getJavaType()).thenReturn(GenericType.LONG);

        CqlConversion conversion = new CqlConversion(DataTypes.listOf(DataTypes.INT),
                DataTypes.listOf(DataTypes.BIGINT), codecRegistry);

        List<Integer> emptyList = Collections.emptyList();
        Object result = conversion.convert(emptyList);

        assertNotNull(result);
        assertTrue(result instanceof List);
        assertTrue(((List<?>) result).isEmpty());
    }

    @Test
    void testConvertCODECUsesCorrectFromDataType() {
        // This test specifically validates the fix at line 276
        // The codec should be retrieved using fromDataType, not toDataType

        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);

        // The key assertion: codecFor should be called with fromDataType (INT) and fromClass (Integer.class)
        when(codecRegistry.codecFor(eq(DataTypes.INT), eq(Integer.class))).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(eq(DataTypes.BIGINT), eq(Long.class))).thenReturn((TypeCodec) bigintCodec);

        when(intCodec.encode(anyInt(), any())).thenReturn(ByteBuffer.allocate(4).putInt(42).flip());
        when(bigintCodec.decode(any(ByteBuffer.class), any())).thenReturn(42L);

        // Call convert_CODEC directly to test the fix
        Object result = CqlConversion.convert_CODEC(42, DataTypes.INT, DataTypes.BIGINT, codecRegistry);

        assertNotNull(result);
        assertEquals(42L, result);

        // Verify that codecFor was called with the correct fromDataType
        verify(codecRegistry).codecFor(eq(DataTypes.INT), eq(Integer.class));
        verify(codecRegistry).codecFor(eq(DataTypes.BIGINT), eq(Long.class));
    }

    @Test
    void testConvertMapWithMixedConversions() {
        // Test map where keys need conversion but values don't
        @SuppressWarnings("unchecked")
        TypeCodec<Integer> intCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<Long> bigintCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<String> textCodec = mock(TypeCodec.class);

        when(codecRegistry.codecFor(eq(DataTypes.INT), eq(Integer.class))).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(eq(DataTypes.BIGINT), eq(Long.class))).thenReturn((TypeCodec) bigintCodec);
        when(codecRegistry.codecFor(DataTypes.INT)).thenReturn((TypeCodec) intCodec);
        when(codecRegistry.codecFor(DataTypes.BIGINT)).thenReturn((TypeCodec) bigintCodec);
        lenient().when(codecRegistry.codecFor(DataTypes.TEXT)).thenReturn((TypeCodec) textCodec);

        when(intCodec.getJavaType()).thenReturn(GenericType.INTEGER);
        when(bigintCodec.getJavaType()).thenReturn(GenericType.LONG);
        lenient().when(textCodec.getJavaType()).thenReturn(GenericType.STRING);

        when(intCodec.encode(anyInt(), any())).thenAnswer(invocation -> {
            Integer value = invocation.getArgument(0);
            return ByteBuffer.allocate(4).putInt(value).flip();
        });
        when(bigintCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            return (long) buffer.getInt();
        });

        CqlConversion conversion = new CqlConversion(DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT),
                DataTypes.mapOf(DataTypes.BIGINT, DataTypes.TEXT), codecRegistry);

        Map<Integer, String> inputMap = new HashMap<>();
        inputMap.put(10, "ten");
        inputMap.put(20, "twenty");

        Object result = conversion.convert(inputMap);

        assertNotNull(result);
        assertTrue(result instanceof Map);
        Map<?, ?> resultMap = (Map<?, ?>) result;
        assertEquals(2, resultMap.size());

        // Verify conversion types
        assertEquals(CqlConversion.Type.MAP, conversion.getConversionTypeList().get(0));
        assertEquals(CqlConversion.Type.CODEC, conversion.getConversionTypeList().get(1)); // key conversion
        assertEquals(CqlConversion.Type.NONE, conversion.getConversionTypeList().get(2)); // value no conversion
    }

    // ========== Direct Codec Conversion Tests ==========

    @Test
    void testConvertTextToTimestampUsingDirectCodec() {
        // This test verifies the TEXT→TIMESTAMP conversion using TEXTMillis_InstantCodec
        // The direct codec path allows TEXT data to be converted to Instant without going through
        // the standard encode/decode ByteBuffer pattern that would fail with UTF-8 text bytes

        @SuppressWarnings("unchecked")
        TypeCodec<String> textCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<java.time.Instant> directCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<java.time.Instant> timestampCodec = mock(TypeCodec.class);

        // Setup codecs for construction phase
        lenient().when(codecRegistry.codecFor(DataTypes.TEXT)).thenReturn((TypeCodec) textCodec);
        lenient().when(codecRegistry.codecFor(DataTypes.TIMESTAMP)).thenReturn((TypeCodec) timestampCodec);

        // Setup TEXT codec for (TEXT, String)
        when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(String.class))).thenReturn((TypeCodec) textCodec);
        when(textCodec.getJavaType()).thenReturn(GenericType.STRING);

        // Setup direct codec for (TEXT, Instant) - this is the key for the fix
        lenient().when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(java.time.Instant.class)))
                .thenReturn((TypeCodec) directCodec);
        lenient().when(directCodec.getJavaType()).thenReturn(GenericType.INSTANT);

        // Setup TIMESTAMP codec for (TIMESTAMP, Instant)
        lenient().when(codecRegistry.codecFor(eq(DataTypes.TIMESTAMP), eq(java.time.Instant.class)))
                .thenReturn((TypeCodec) timestampCodec);
        lenient().when(timestampCodec.getJavaType()).thenReturn(GenericType.INSTANT);

        // Mock encode behavior for TEXT codec - encodes string to UTF-8 bytes
        when(textCodec.encode(anyString(), any())).thenAnswer(invocation -> {
            String value = invocation.getArgument(0);
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            return ByteBuffer.wrap(bytes);
        });

        // Mock decode behavior for direct codec - parses milliseconds string to Instant
        when(directCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            String millisStr = java.nio.charset.StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
            long millis = Long.parseLong(millisStr);
            return java.time.Instant.ofEpochMilli(millis);
        });

        CqlConversion conversion = new CqlConversion(DataTypes.TEXT, DataTypes.TIMESTAMP, codecRegistry);

        // Test conversion of milliseconds string to Instant
        String inputMillis = "1087383600000"; // 2004-06-15T15:00:00Z
        Object result = conversion.convert(inputMillis);

        assertNotNull(result);
        assertTrue(result instanceof java.time.Instant);
        java.time.Instant resultInstant = (java.time.Instant) result;
        assertEquals(1087383600000L, resultInstant.toEpochMilli());
        assertEquals(CqlConversion.Type.CODEC, conversion.getConversionTypeList().get(0));

        // Verify that the direct codec was used
        verify(directCodec, times(1)).decode(any(ByteBuffer.class), any());
    }

    @Test
    void testConvertTextToTimestampFallbackToStandardConversion() {
        // This test verifies that when no direct codec is available, the standard conversion is used

        @SuppressWarnings("unchecked")
        TypeCodec<String> textCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<java.time.Instant> timestampCodec = mock(TypeCodec.class);

        // Setup codecs for construction phase
        lenient().when(codecRegistry.codecFor(DataTypes.TEXT)).thenReturn((TypeCodec) textCodec);
        lenient().when(codecRegistry.codecFor(DataTypes.TIMESTAMP)).thenReturn((TypeCodec) timestampCodec);

        // Setup TEXT codec for (TEXT, String)
        when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(String.class))).thenReturn((TypeCodec) textCodec);
        when(textCodec.getJavaType()).thenReturn(GenericType.STRING);

        // No direct codec available - codecFor(TEXT, Instant) throws exception
        when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(java.time.Instant.class)))
                .thenThrow(new IllegalArgumentException("No codec found"));

        // Setup TIMESTAMP codec for (TIMESTAMP, Instant)
        when(codecRegistry.codecFor(eq(DataTypes.TIMESTAMP), eq(java.time.Instant.class)))
                .thenReturn((TypeCodec) timestampCodec);
        when(timestampCodec.getJavaType()).thenReturn(GenericType.INSTANT);

        // Mock encode behavior for TEXT codec
        when(textCodec.encode(anyString(), any())).thenAnswer(invocation -> {
            String value = invocation.getArgument(0);
            // In real scenario, this would encode to UTF-8 bytes
            return ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        });

        // Mock decode behavior for TIMESTAMP codec - in real scenario this would fail with UTF-8 bytes
        // but for this test we'll make it work to verify the fallback path is taken
        when(timestampCodec.decode(any(ByteBuffer.class), any())).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            String millisStr = java.nio.charset.StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
            long millis = Long.parseLong(millisStr);
            return java.time.Instant.ofEpochMilli(millis);
        });

        CqlConversion conversion = new CqlConversion(DataTypes.TEXT, DataTypes.TIMESTAMP, codecRegistry);

        String inputMillis = "1087383600000";
        Object result = conversion.convert(inputMillis);

        assertNotNull(result);
        assertTrue(result instanceof java.time.Instant);

        // Verify that standard conversion path was used (both codecs called)
        verify(textCodec, times(1)).encode(anyString(), any());
        verify(timestampCodec, times(1)).decode(any(ByteBuffer.class), any());
    }

    @Test
    void testConvertTextToTimestampWithNullValue() {
        // Test that null values are handled correctly in direct codec conversion

        @SuppressWarnings("unchecked")
        TypeCodec<String> textCodec = mock(TypeCodec.class);
        @SuppressWarnings("unchecked")
        TypeCodec<java.time.Instant> directCodec = mock(TypeCodec.class);

        // Setup codecs for construction phase
        lenient().when(codecRegistry.codecFor(DataTypes.TEXT)).thenReturn((TypeCodec) textCodec);
        lenient().when(codecRegistry.codecFor(DataTypes.TIMESTAMP)).thenReturn((TypeCodec) directCodec);

        lenient().when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(String.class))).thenReturn((TypeCodec) textCodec);
        lenient().when(codecRegistry.codecFor(eq(DataTypes.TEXT), eq(java.time.Instant.class)))
                .thenReturn((TypeCodec) directCodec);
        lenient().when(textCodec.getJavaType()).thenReturn(GenericType.STRING);
        lenient().when(directCodec.getJavaType()).thenReturn(GenericType.INSTANT);

        lenient().when(textCodec.encode(isNull(), any())).thenReturn(null);
        lenient().when(directCodec.decode(isNull(), any())).thenReturn(null);

        CqlConversion conversion = new CqlConversion(DataTypes.TEXT, DataTypes.TIMESTAMP, codecRegistry);

        Object result = conversion.convert(null);
        assertNull(result);
    }
}
