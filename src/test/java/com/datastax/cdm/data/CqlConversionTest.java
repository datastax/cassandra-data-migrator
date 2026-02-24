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

}
