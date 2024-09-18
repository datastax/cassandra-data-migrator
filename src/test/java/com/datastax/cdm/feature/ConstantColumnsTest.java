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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.KnownProperties;

public class ConstantColumnsTest extends CommonMocks {

    ConstantColumns feature;
    List<Class> expectedBindClasses;

    // String standardValues = "'abcd',1234,543";
    // String standardRegex = ",";
    // List<Class> standardBindClasses =
    // standardDataTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList());
    // List<String> standardValuesAsList = Arrays.asList(standardValues.split(standardRegex));

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        // setTestVariables();
        commonSetupWithoutDefaultClassVariables(false, true, false);
        // setTestWhens();
        feature = new ConstantColumns();
        expectedBindClasses = constantColumnTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList());

        when(propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES)).thenReturn(constantColumns);
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES))
                .thenReturn(String.join(",", constantColumnValues));
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX)).thenReturn(",");

    }

    // private void setTestVariables() {
    // targetValueColumns = new ArrayList<>(originValueColumns);
    // targetValueColumns.addAll(standardNames);
    // targetValueColumnTypes = new ArrayList<>(originValueColumnTypes);
    // targetValueColumnTypes.addAll(standardDataTypes);
    // }

    // private void setTestWhens(){
    // when(targetCodec.parse(anyString())).thenReturn(any());
    // }

    @Test
    public void smokeTest_loadProperties() {
        feature.loadProperties(propertyHelper);
        assertAll(() -> assertTrue(feature.isEnabled()),
                () -> assertEquals(constantColumns, feature.getNames(), "names"),
                () -> assertEquals(constantColumnValues, feature.getValues(), "values"));
    }

    @Test
    public void smokeTest_initializeAndValidate() {

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(() -> assertTrue(valid, "correct validation"),
                () -> assertEquals(expectedBindClasses, feature.getBindClasses(), "bind classes"));
    }

    @Test
    public void smokeTest_disabledFeature() {
        when(propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES)).thenReturn(null);
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES)).thenReturn("");
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX)).thenReturn("");

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"),
                () -> assertTrue(feature.getNames().isEmpty(), "empty names"),
                () -> assertTrue(feature.getValues().isEmpty(), "empty values"),
                () -> assertTrue(feature.getBindClasses().isEmpty(), "empty bindClasses")
        );
    }

    @Test
    public void testEmptyConstantColumnValue() {
        String emptyValue = ",1234,543";
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES)).thenReturn(emptyValue);

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(() -> assertFalse(valid, "null string is invalid"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"));
    }

    @Test
    public void testMismatchedConstantColumnNamesAndValues() {
        List<String> extraName = Arrays.asList("const1", "const2", "const3", "const4");
        when(propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES)).thenReturn(extraName);

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(() -> assertFalse(valid, "Validation should fail with mismatched names and values"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"));
    }

    @Test
    public void testMissingConstantColumnInTargetTable() {
        List<Class> bindClasses = new ArrayList<>(expectedBindClasses);
        bindClasses.set(1, null);
        when(targetTable.extendColumns(constantColumns)).thenReturn(bindClasses);

        feature.loadProperties(propertyHelper);
        assertFalse(feature.initializeAndValidate(originTable, targetTable),
                "Validation should fail with a missing constant column in the target table");
    }

    @Test
    public void testMissingSplitRegex() {
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX)).thenReturn(null);

        assertThrows(RuntimeException.class, () -> feature.loadProperties(propertyHelper), "Exception should be thrown for missing split regex");
    }

    @Test
    public void testWrongSplitRegex() {
        when(propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX)).thenReturn(";");

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertFalse(valid, "validation should fail as the number of values will not match the number of names"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled")
        );
    }

    @Test
    public void testMissingConstantColumnInBindClasses() {
        when(targetTable.extendColumns(constantColumns)).thenReturn(Arrays.asList(CqlData.getBindClass(constantColumnTypes.get(0)), null, CqlData.getBindClass(constantColumnTypes.get(2))));

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertFalse(valid, "Validation should fail with a missing constant column in the bind classes"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled")
        );
    }

    @Test
    public void testEmptyConstantColumnValueInBindClasses() {
        when(targetTable.extendColumns(constantColumns)).thenReturn(Arrays.asList(CqlData.getBindClass(constantColumnTypes.get(0)), CqlData.getBindClass(constantColumnTypes.get(1)), null));

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertFalse(valid, "Validation should fail with an empty constant column value in the bind classes"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled")
        );
    }

    @Test
    public void testConstantColumnValueCannotBeParsed() {
        when(targetCodec.parse(anyString())).thenThrow(new RuntimeException("Invalid value"));

        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertFalse(valid, "Validation should fail with a constant column value that cannot be parsed"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled")
        );
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

}
