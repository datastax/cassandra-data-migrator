package com.datastax.cdm.data;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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

        originValueColumns.addAll(Arrays.asList("parameter-value","PaRaMeTeR-Value-MiXedCaSE"));
        originValueColumnTypes.addAll(Arrays.asList(DataTypes.INT, DataTypes.TEXT));

        originToTargetNameList = Arrays.asList("parameter-value:parameter_value","PaRaMeTeR-Value-MiXedCaSE:parameter_value_standard_case");

        targetValueColumns.addAll(Arrays.asList("parameter_value","parameter_value_standard_case"));
        targetValueColumnTypes.addAll(Arrays.asList(DataTypes.INT, DataTypes.TEXT));
    }

    @Test
    public void originToTarget() {
        Map<String,String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, originTable, targetTable);

        assertAll(
                () -> assertEquals("parameter_value", map.get("parameter-value"), "encapsulated name"),
                () -> assertEquals("parameter_value_standard_case", map.get("PaRaMeTeR-Value-MiXedCaSE"), "Mixed and complete rename"),
                () -> assertEquals(targetColumnNames.size(), map.size(), "Map size should match origin column count")
        );
    }

    @Test
    public void targetToOrigin() {
        Map<String, String> map = DataUtility.getThisToThatColumnNameMap(propertyHelper, targetTable, originTable);

        assertAll(
                () -> assertEquals("parameter-value", map.get("parameter_value"), "encapsulated name"),
                () -> assertEquals("PaRaMeTeR-Value-MiXedCaSE", map.get("parameter_value_standard_case"), "Mixed and complete rename"),
                () -> assertEquals(originColumnNames.size(), map.size(), "Map size should match target column count")
        );
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
}
