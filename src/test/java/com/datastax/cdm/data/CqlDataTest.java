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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;

public class CqlDataTest {
    @Test
    public void testToType_Primitive() {
        assertEquals(CqlData.Type.PRIMITIVE, CqlData.toType(DataTypes.TEXT));
    }

    @Test
    public void testToType_List() {
        ListType listType = mock(ListType.class);
        assertEquals(CqlData.Type.LIST, CqlData.toType(listType));
    }

    @Test
    public void testToType_Set() {
        SetType setType = mock(SetType.class);
        assertEquals(CqlData.Type.SET, CqlData.toType(setType));
    }

    @Test
    public void testToType_Map() {
        MapType mapType = mock(MapType.class);
        assertEquals(CqlData.Type.MAP, CqlData.toType(mapType));
    }

    @Test
    public void testToType_Tuple() {
        TupleType tupleType = mock(TupleType.class);
        assertEquals(CqlData.Type.TUPLE, CqlData.toType(tupleType));
    }

    @Test
    public void testToType_UDT() {
        UserDefinedType udt = mock(UserDefinedType.class);
        assertEquals(CqlData.Type.UDT, CqlData.toType(udt));
    }

    @Test
    public void testToType_Vector() {
        VectorType vectorType = mock(VectorType.class);
        assertEquals(CqlData.Type.VECTOR, CqlData.toType(vectorType));
    }

    @Test
    public void testIsPrimitive() {
        assertTrue(CqlData.isPrimitive(DataTypes.TEXT));
        DataType unknown = mock(DataType.class);
        assertFalse(CqlData.isPrimitive(unknown));
    }

    @Test
    public void testIsCollection() {
        assertTrue(CqlData.isCollection(mock(ListType.class)));
        assertTrue(CqlData.isCollection(mock(SetType.class)));
        assertTrue(CqlData.isCollection(mock(MapType.class)));
        assertTrue(CqlData.isCollection(mock(TupleType.class)));
        assertTrue(CqlData.isCollection(mock(UserDefinedType.class)));
        assertTrue(CqlData.isCollection(mock(VectorType.class)));
        assertFalse(CqlData.isCollection(DataTypes.TEXT));
    }

    @Test
    public void testIsFrozen_Primitive() {
        assertFalse(CqlData.isFrozen(DataTypes.TEXT));
    }

    @Test
    public void testIsFrozen_UDT() {
        UserDefinedType udt = mock(UserDefinedType.class);
        when(udt.isFrozen()).thenReturn(true);
        assertTrue(CqlData.isFrozen(udt));
    }

    @Test
    public void testGetBindClass_Primitive() {
        assertEquals(String.class, CqlData.getBindClass(DataTypes.TEXT));
    }

    @Test
    public void testGetBindClass_List() {
        ListType listType = mock(ListType.class);
        assertEquals(java.util.List.class, CqlData.getBindClass(listType));
    }

    @Test
    public void testExtractDataTypesFromCollection_List() {
        ListType listType = mock(ListType.class);
        when(listType.getElementType()).thenReturn(DataTypes.TEXT);
        assertEquals(Collections.singletonList(DataTypes.TEXT), CqlData.extractDataTypesFromCollection(listType));
    }

    @Test
    public void testExtractDataTypesFromCollection_Map() {
        MapType mapType = mock(MapType.class);
        when(mapType.getKeyType()).thenReturn(DataTypes.TEXT);
        when(mapType.getValueType()).thenReturn(DataTypes.INT);
        assertEquals(Arrays.asList(DataTypes.TEXT, DataTypes.INT), CqlData.extractDataTypesFromCollection(mapType));
    }

    @Test
    public void testGetFormattedContent_Primitive() {
        assertEquals("abc", CqlData.getFormattedContent(CqlData.Type.PRIMITIVE, "abc"));
    }

    @Test
    public void testGetFormattedContent_Null() {
        assertEquals("", CqlData.getFormattedContent(CqlData.Type.PRIMITIVE, null));
    }

    @Test
    public void testGetFormattedContent_List() {
        String result = CqlData.getFormattedContent(CqlData.Type.LIST, Arrays.asList("a", "b"));
        assertTrue(result.startsWith("["));
    }

    @Test
    public void testGetFormattedContent_Map() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "v");
        String result = CqlData.getFormattedContent(CqlData.Type.MAP, map);
        assertTrue(result.startsWith("{"));
    }

    @Test
    public void testGetFormattedContent_UDT() {
        UdtValue udtValue = mock(UdtValue.class);
        when(udtValue.getFormattedContents()).thenReturn("{udt}");
        assertEquals("{udt}", CqlData.getFormattedContent(CqlData.Type.UDT, udtValue));
    }
}
