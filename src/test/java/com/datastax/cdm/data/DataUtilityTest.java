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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class DataUtilityTest extends CommonMocks {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        setTestVariables();
        commonSetupWithoutDefaultClassVariables();
    }

    private void setTestVariables() {
        originValueColumns = new ArrayList<>(originValueColumns);
        originValueColumnTypes = new ArrayList<>(originValueColumnTypes);
        // Target values are not set until commonSetup(), so default them from origin
        targetValueColumns = new ArrayList<>(originValueColumns);
        targetValueColumnTypes = new ArrayList<>(originValueColumnTypes);

        originValueColumns.addAll(Arrays.asList("parameter-value", "PaRaMeTeR-Value-MiXedCaSE"));
        originValueColumnTypes.addAll(Arrays.asList(DataTypes.INT, DataTypes.TEXT));

        originToTargetNameList = Arrays.asList("parameter-value:parameter_value",
                "PaRaMeTeR-Value-MiXedCaSE:parameter_value_standard_case");

        targetValueColumns.addAll(Arrays.asList("parameter_value", "parameter_value_standard_case"));
        targetValueColumnTypes.addAll(Arrays.asList(DataTypes.INT, DataTypes.TEXT));
    }

    @Test
    public void originToTarget() {
        Map<String, String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, originTable, targetTable);

        assertAll(() -> assertEquals("parameter_value", map.get("parameter-value"), "encapsulated name"),
                () -> assertEquals("parameter_value_standard_case", map.get("PaRaMeTeR-Value-MiXedCaSE"),
                        "Mixed and complete rename"),
                () -> assertEquals(targetColumnNames.size(), map.size(), "Map size should match origin column count"));
    }

    @Test
    public void targetToOrigin() {
        Map<String, String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, targetTable, originTable);

        assertAll(() -> assertEquals("parameter-value", map.get("parameter_value"), "encapsulated name"),
                () -> assertEquals("PaRaMeTeR-Value-MiXedCaSE", map.get("parameter_value_standard_case"),
                        "Mixed and complete rename"),
                () -> assertEquals(originColumnNames.size(), map.size(), "Map size should match target column count"));
    }

    @Test
    public void columnOnThisNotThat() {
        String extraColumn = "extraColumn";
        originColumnNames.add(extraColumn);

        Map<String, String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, originTable, targetTable);
        // When there is no corresponding value on That, the value should be null
        assertNull(map.get(extraColumn));
    }

    @Test
    public void columnOnThatNotThis() {
        String extraColumn = "extraColumn";
        targetColumnNames.add(extraColumn);

        Map<String, String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, originTable, targetTable);
        // Loop over the map entries, extraColumn should not be present
        for (Map.Entry<String, String> entry : map.entrySet()) {
            assertNotEquals(extraColumn, entry.getValue());
        }
    }

    @Test
    public void diffTest() {
        assertFalse(DataUtility.diff(null, null));
        assertFalse(DataUtility.diff("Hello", "Hello"));
        assertTrue(DataUtility.diff(null, "Hello"));
        assertTrue(DataUtility.diff("Hello", null));
        assertTrue(DataUtility.diff("", "Hello"));
        assertTrue(DataUtility.diff("hello", "Hello"));
    }

    @Test
    public void extractObjectsFromCollectionTest() {
        List<Object> expected = Arrays.asList(1, 2, 3);
        List<Object> actualList = new ArrayList<>();
        actualList.add(1);
        actualList.add(2);
        actualList.add(3);
        assertEquals(expected, DataUtility.extractObjectsFromCollection(actualList));

        Set<Object> actualSet = new HashSet<>();
        actualSet.add(1);
        actualSet.add(2);
        actualSet.add(3);
        assertEquals(expected, DataUtility.extractObjectsFromCollection(actualSet));

        Map<String, String> actualMap = Map.of("1", "one", "2", "two", "3", "three");
        List<Object> expectedMap = new ArrayList<>(actualMap.entrySet());
        assertEquals(expectedMap, DataUtility.extractObjectsFromCollection(actualMap));
    }

    @Test
    public void getMyClassMethodLineTestCDMClass() {
        Exception ex = new Exception();
        ex.setStackTrace(new StackTraceElement[] { new StackTraceElement("com.datastax.cdm.data.DataUtilityTest",
                "getMyClassMethodLineTest", "DataUtilityTest.java", 0) });
        assertEquals("com.datastax.cdm.data.DataUtilityTest.getMyClassMethodLineTest:0",
                DataUtility.getMyClassMethodLine(ex));
    }

    @Test
    public void getMyClassMethodLineTestOtherClass() {
        Exception ex = new Exception();
        ex.setStackTrace(new StackTraceElement[] { new StackTraceElement("com.datastax.other.SomeClass",
                "getMyClassMethodLineTest", "SomeClass.java", 0) });
        assertEquals("com.datastax.other.SomeClass.getMyClassMethodLineTest:0", DataUtility.getMyClassMethodLine(ex));
    }

    @Test
    public void getMyClassMethodLineTestUnknown() {
        Exception ex = new Exception();
        ex.setStackTrace(new StackTraceElement[] {});
        assertEquals("Unknown", DataUtility.getMyClassMethodLine(ex));
    }

    @Test
    public void generateSCBOrigin() throws IOException {
        File scb = DataUtility.generateSCB("localhost", "9042", "trust123", "./pom.xml", "key123", "./pom.xml",
                PKFactory.Side.ORIGIN, 0);
        assertNotNull(scb);
        File file = new File(PKFactory.Side.ORIGIN + "_" + Long.toString(0) + DataUtility.SCB_FILE_NAME);
        assertTrue(file.exists());

        DataUtility.deleteGeneratedSCB(0, 0);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        assertFalse(file.exists());
    }

    @Test
    public void generateSCBTarget() throws IOException {
        File scb = DataUtility.generateSCB("localhost", "9042", "trust123", "./pom.xml", "key123", "./pom.xml",
                PKFactory.Side.TARGET, 0);
        assertNotNull(scb);
        File file = new File(PKFactory.Side.TARGET + "_" + Long.toString(0) + DataUtility.SCB_FILE_NAME);
        assertTrue(file.exists());

        DataUtility.deleteGeneratedSCB(0, 0);
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

        assertFalse(file.exists());
    }
}
