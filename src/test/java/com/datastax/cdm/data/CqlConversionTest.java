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

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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

}
