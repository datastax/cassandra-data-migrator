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
package com.datastax.cdm.feature;

import static org.apache.hadoop.shaded.com.google.common.base.CharMatcher.any;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;

public class ExplodeMapTest {

    ExplodeMap feature;

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    CqlTable originTable;

    @Mock
    CqlTable targetTable;

    @Mock
    List<CqlConversion> conversionList;

    @Mock
    CqlConversion conversion;

    @Mock
    MutableCodecRegistry codecRegistry;

    String standardMapColumnName = "map_col";
    List<String> standardOriginNames = Arrays.asList("key", "val", standardMapColumnName);
    List<DataType> standardOriginTypes = Arrays.asList(DataTypes.TIMESTAMP, DataTypes.INT,
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE));

    String standardKeyColumnName = "map_key";
    String standardValueColumnName = "map_val";
    List<String> standardTargetNames = Arrays.asList("key", "val", standardKeyColumnName, standardValueColumnName);
    List<DataType> standardTargetTypes = Arrays.asList(DataTypes.TIMESTAMP, DataTypes.INT, DataTypes.TEXT,
            DataTypes.DOUBLE);

    @BeforeEach
    public void setup() {
        feature = new ExplodeMap();
        MockitoAnnotations.openMocks(this);

        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME))
                .thenReturn(standardMapColumnName);
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME))
                .thenReturn(standardKeyColumnName);
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME))
                .thenReturn(standardValueColumnName);

        when(originTable.isOrigin()).thenReturn(true);
        when(originTable.extendColumns(Collections.singletonList(standardMapColumnName)))
                .thenReturn(Collections.singletonList(CqlData.getBindClass(standardOriginTypes.get(2))));

        when(targetTable.isOrigin()).thenReturn(false);
        when(targetTable.extendColumns(Arrays.asList(standardKeyColumnName, standardValueColumnName)))
                .thenReturn(Arrays.asList(CqlData.getBindClass(standardTargetTypes.get(2)),
                        CqlData.getBindClass(standardTargetTypes.get(3))));

        for (int i = 0; i < standardOriginNames.size(); i++) {
            when(originTable.getColumnNames(false)).thenReturn(standardOriginNames);
            when(originTable.indexOf(standardOriginNames.get(i))).thenReturn(i);
            when(originTable.getDataType(standardOriginNames.get(i))).thenReturn(standardOriginTypes.get(i));
            when(originTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardOriginTypes.get(i)));
        }

        for (int i = 0; i < standardTargetNames.size(); i++) {
            when(targetTable.getColumnNames(false)).thenReturn(standardTargetNames);
            when(targetTable.indexOf(standardTargetNames.get(i))).thenReturn(i);
            when(targetTable.getDataType(standardTargetNames.get(i))).thenReturn(standardTargetTypes.get(i));
            when(targetTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardTargetTypes.get(i)));
        }

        when(targetTable.getConversions()).thenReturn(conversionList);
        when(conversionList.get(anyInt())).thenReturn(conversion);
        when(conversion.convert(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(targetTable.getCodecRegistry()).thenReturn(codecRegistry);
    }

    @Test
    public void smokeTest_loadProperties() {
        boolean loaded = feature.loadProperties(propertyHelper);

        assertAll(() -> assertTrue(loaded, "properties are loaded and valid"), () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(standardMapColumnName, feature.getOriginColumnName(), "origin name"),
                () -> assertEquals(standardKeyColumnName, feature.getKeyColumnName(), "key name"),
                () -> assertEquals(standardValueColumnName, feature.getValueColumnName(), "value name"));
    }

    @Test
    public void smokeTest_initializeAndValidate() {
        feature.loadProperties(propertyHelper);

        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertAll(() -> assertTrue(valid, "configuration is valid"),
                () -> assertEquals(standardOriginNames.indexOf(standardMapColumnName), feature.getOriginColumnIndex(),
                        "origin index"),
                () -> assertEquals(standardTargetNames.indexOf(standardKeyColumnName), feature.getKeyColumnIndex(),
                        "key index"),
                () -> assertEquals(standardTargetNames.indexOf(standardValueColumnName), feature.getValueColumnIndex(),
                        "value index"));
    }

    @Test
    public void smokeTest_disabledFeature() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME)).thenReturn("");
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME)).thenReturn("");
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME)).thenReturn("");

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"),

                () -> assertEquals("", feature.getOriginColumnName(), "origin name"),
                () -> assertEquals(-1, feature.getOriginColumnIndex(), "origin index"),

                () -> assertEquals("", feature.getKeyColumnName(), "key name"),
                () -> assertEquals(-1, feature.getKeyColumnIndex(), "key index"),

                () -> assertEquals("", feature.getValueColumnName(), "value name"),
                () -> assertEquals(-1, feature.getValueColumnIndex(), "value index")
        );
    }

    @Test
    public void testMissingOriginColumnName() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME)).thenReturn(null);
        assertEquals("", feature.getOriginColumnName(propertyHelper));
    }

    @Test
    public void testMissingKeyColumnName() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME)).thenReturn(null);
        assertEquals("", feature.getKeyColumnName(propertyHelper));
    }

    @Test
    public void testMissingValueColumnName() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME)).thenReturn(null);
        assertEquals("", feature.getValueColumnName(propertyHelper));
    }

    @Test
    public void testInvalidOriginColumn_enabledFeatureIsDisabled() {
        when(originTable.getDataType(standardMapColumnName)).thenReturn(DataTypes.TEXT);
        feature.loadProperties(propertyHelper);

        boolean enabledBeforeValidation = feature.isEnabled();
        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertTrue(enabledBeforeValidation, "feature was enabled"),
                () -> assertFalse(valid, "configuration is not valid"),
                () -> assertFalse(feature.isEnabled(), "feature should now be disabled")
                );
    }

    @Test
    public void testMissingMapColumnInOriginTable() {
        when(originTable.extendColumns(Collections.singletonList(standardMapColumnName))).thenReturn(Collections.singletonList(null));
        feature.loadProperties(propertyHelper);

        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertFalse(valid, "Feature should be invalid");
    }

    @Test
    public void testMissingKeyColumnInTargetTable() {
        when(targetTable.extendColumns(Arrays.asList(standardKeyColumnName,standardValueColumnName))).thenReturn(Arrays.asList(null,CqlData.getBindClass(standardTargetTypes.get(3))));
        feature.loadProperties(propertyHelper);

        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertFalse(valid, "Feature should be invalid");
    }

    @Test
    public void testMissingValueColumnInTargetTable() {
        when(targetTable.extendColumns(Arrays.asList(standardKeyColumnName,standardValueColumnName))).thenReturn(Arrays.asList(CqlData.getBindClass(standardTargetTypes.get(2)), null));

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertFalse(valid, "Feature should be invalid");
    }

    @Test
    public void testMissingOriginColumn() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME)).thenReturn("");
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void testMissingKeyColumn() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME)).thenReturn("");
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void testMissingValueColumn() {
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME)).thenReturn("");
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void testOriginIsNull() {
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(null, targetTable));
    }

    @Test
    public void testOriginIsTarget() {
        when(originTable.isOrigin()).thenReturn(false);
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    public void testTargetIsNull() {
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, null));
    }

    @Test
    public void testTargetIsOrigin() {
        when(targetTable.isOrigin()).thenReturn(true);
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    public void testExplode_noConversion() {
        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Map<Object, Object> testMap = new HashMap<>();
        testMap.put("key1", 10);
        testMap.put("key2", 20);
        Set<Map.Entry<Object, Object>> testEntries = testMap.entrySet();

        assertEquals(testEntries, feature.explode(testMap));
    }

    @Test
    public void testExplode_convertKey() {
        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Map<Object, Object> testMap = new HashMap<>();
        testMap.put("key1", 10);
        testMap.put("key2", 20);

        Map<Object, Object> convertedMap = new HashMap<>();
        convertedMap.put("AAAkey1", 10);
        convertedMap.put("AAAkey2", 20);

        // Create a custom implementation of CqlConversion - we want to confirm that the conversion
        // is actually called, rather than just passing through the EntrySet.
        CqlConversion conversion = new CqlConversion(DataTypes.TEXT, DataTypes.TEXT, codecRegistry) {
            @Override
            public Object convert(Object value) {
                return "AAA" + value;
            }
        };
        feature.keyConversion = conversion;

        assertEquals(convertedMap.entrySet(), feature.explode(testMap));
    }

    @Test
    public void testExplode_convertValue() {
        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Map<Object, Object> testMap = new HashMap<>();
        testMap.put("key1", 10);
        testMap.put("key2", 20);

        Map<Object, Object> convertedMap = new HashMap<>();
        convertedMap.put("key1", 100);
        convertedMap.put("key2", 200);

        // Create a custom implementation of CqlConversion - we want to confirm that the conversion
        // is actually called, rather than just passing through the EntrySet.
        CqlConversion conversion = new CqlConversion(DataTypes.INT, DataTypes.INT, codecRegistry) {
            @Override
            public Object convert(Object value) {
                return (Integer) value * 10;
            }
        };
        feature.valueConversion = conversion;

        assertEquals(convertedMap.entrySet(), feature.explode(testMap));
    }
}
