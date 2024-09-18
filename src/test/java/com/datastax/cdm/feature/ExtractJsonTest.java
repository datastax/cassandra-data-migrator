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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ExtractJsonTest {

    ExtractJson feature;

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    CqlTable originTable;

    @Mock
    CqlTable targetTable;

    List<String> standardOriginNames = Arrays.asList("id", "content");
    List<String> standardTargetNames = Arrays.asList("id", "age");
    List<DataType> standardOriginTypes = Arrays.asList(DataTypes.TEXT, DataTypes.TEXT);
    List<DataType> standardTargetTypes = Arrays.asList(DataTypes.TEXT, DataTypes.TEXT);

    String standardOriginName = "content";
    String standardTargetName = "age";
    String mappedTargetName = "personAge:person_age";

    @BeforeEach
    public void setup() {
        feature = new ExtractJson();
        MockitoAnnotations.openMocks(this);

        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME)).thenReturn(standardOriginName);
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING))
                .thenReturn(standardTargetName);

        when(originTable.getKeyspaceTable()).thenReturn("ORIGIN_TABLE");
        when(originTable.isOrigin()).thenReturn(true);
        when(originTable.extendColumns(Collections.singletonList(standardOriginName)))
                .thenReturn(Collections.singletonList(CqlData.getBindClass(standardOriginTypes.get(1))));

        when(targetTable.getKeyspaceTable()).thenReturn("TARGET_TABLE");
        when(targetTable.isOrigin()).thenReturn(false);
        when(targetTable.extendColumns(Collections.singletonList(standardTargetName)))
                .thenReturn(Collections.singletonList(CqlData.getBindClass(standardTargetTypes.get(1))));

        for (int i = 0; i < standardOriginNames.size(); i++) {
            when(originTable.getColumnNames(false)).thenReturn(standardOriginNames);
            when(originTable.indexOf(standardOriginNames.get(i))).thenReturn(i);
            when(originTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardOriginTypes.get(i)));
        }

        for (int i = 0; i < standardTargetNames.size(); i++) {
            when(targetTable.getColumnNames(false)).thenReturn(standardTargetNames);
            when(targetTable.indexOf(standardTargetNames.get(i))).thenReturn(i);
            when(targetTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardTargetTypes.get(i)));
        }
    }

    @Test
    public void loadProperties() {
        boolean loaded = feature.loadProperties(propertyHelper);

        assertAll(() -> assertTrue(loaded, "properties are loaded and valid"), () -> assertTrue(feature.isEnabled()),
                () -> assertFalse(feature.overwriteTarget()),
                () -> assertEquals(standardTargetName, feature.getTargetColumnName()));
    }

    @Test
    public void loadPropertiesWithMapping() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING)).thenReturn(mappedTargetName);
        boolean loaded = feature.loadProperties(propertyHelper);

        assertAll(
                () -> assertTrue(loaded, "properties are loaded and valid"),
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals("person_age", feature.getTargetColumnName())
        );
    }

    @Test
    public void loadPropertiesException() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> feature.loadProperties(null));
        assertTrue(thrown.getMessage().contains("helper is null"));
    }

    @Test
    public void loadPropertiesOriginError() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME)).thenReturn(null);
        assertFalse(feature.loadProperties(propertyHelper), "Origin column name is not set");
    }

    @Test
    public void loadPropertiesTargetError() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING)).thenReturn(null);
        assertFalse(feature.loadProperties(propertyHelper), "Target column name is not set");
    }

    @Test
    public void initializeAndValidate() {
        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertAll(() -> assertTrue(valid, "configuration is valid"),
                () -> assertEquals(standardOriginNames.indexOf(standardOriginName), feature.getOriginColumnIndex(),
                        "origin index"),
                () -> assertEquals(standardTargetNames.indexOf(standardTargetName), feature.getTargetColumnIndex(),
                        "target index"));
    }

    @Test
    public void extractNull() throws JsonMappingException, JsonProcessingException {
        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertNull(feature.extract("{\"name\":\"Pravin\"}"));
        assertNull(feature.extract("{}"));
        assertNull(feature.extract(""));
    }

    @Test
    public void disabledFeature() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME)).thenReturn("");
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING)).thenReturn("");

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"),

                () -> assertEquals("", feature.getTargetColumnName(), "target name"),
                () -> assertEquals(-1, feature.getTargetColumnIndex(), "target index"),
                () -> assertEquals(-1, feature.getOriginColumnIndex(), "origin index")
        );

        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING)).thenReturn(null);
        assertEquals("", feature.getTargetColumnName(), "target name");
    }

    @Test
    public void initializeAndValidateExceptionOriginNull() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> feature.initializeAndValidate(null, targetTable));
        assertTrue(thrown.getMessage().contains("Origin table and/or Target table is null"));
    }

    @Test
    public void initializeAndValidateExceptionTargetNull() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> feature.initializeAndValidate(originTable, null));
        assertTrue(thrown.getMessage().contains("Origin table and/or Target table is null"));
    }

    @Test
    public void initializeAndValidateExceptionOriginColumn() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME)).thenReturn("incorrect_column");

        feature.loadProperties(propertyHelper);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    public void initializeAndValidateExceptionTargetColumn() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING)).thenReturn("incorrect_column");

        feature.loadProperties(propertyHelper);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    public void initializeAndValidateExceptionOriginIncorrect() {
        when(originTable.isOrigin()).thenReturn(false);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
        assertTrue(thrown.getMessage().contains("ORIGIN_TABLE is not an origin table"));
    }

    @Test
    public void initializeAndValidateExceptionTargetIncorrect() {
        when(targetTable.isOrigin()).thenReturn(true);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
        assertTrue(thrown.getMessage().contains("TARGET_TABLE is not a target table"));
    }

    @Test
    public void invalidFeature() {
        when(propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME)).thenReturn("");

        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled")
        );
    }

}
