package com.datastax.cdm.feature;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExplodeMapTest {

    ExplodeMap feature;

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    CqlTable originTable;

    @Mock
    CqlTable targetTable;

    String standardMapColumnName = "map_col";
    List<String> standardOriginNames = Arrays.asList("key","val",standardMapColumnName);
    List<DataType> standardOriginTypes = Arrays.asList(DataTypes.TIMESTAMP, DataTypes.INT, DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE));

    String standardKeyColumnName = "map_key";
    String standardValueColumnName = "map_val";
    List<String> standardTargetNames = Arrays.asList("key","val",standardKeyColumnName,standardValueColumnName);
    List<DataType> standardTargetTypes = Arrays.asList(DataTypes.TIMESTAMP, DataTypes.INT, DataTypes.TEXT, DataTypes.DOUBLE);

    @BeforeEach
    public void setup() {
        feature = new ExplodeMap();

        propertyHelper = mock(IPropertyHelper.class);
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME)).thenReturn(standardMapColumnName);
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME)).thenReturn(standardKeyColumnName);
        when(propertyHelper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME)).thenReturn(standardValueColumnName);

        originTable = mock(CqlTable.class);
        when(originTable.isOrigin()).thenReturn(true);
        when(originTable.extendColumns(Collections.singletonList(standardMapColumnName))).
                thenReturn(Collections.singletonList(CqlData.getBindClass(standardOriginTypes.get(2))));

        targetTable = mock(CqlTable.class);
        when(targetTable.isOrigin()).thenReturn(false);
        when(targetTable.extendColumns(Arrays.asList(standardKeyColumnName,standardValueColumnName)))
                .thenReturn(Arrays.asList(CqlData.getBindClass(standardTargetTypes.get(2)), CqlData.getBindClass(standardTargetTypes.get(3))));

        for (int i = 0; i< standardOriginNames.size(); i++) {
            when(originTable.getColumnNames(false)).thenReturn(standardOriginNames);
            when(originTable.indexOf(standardOriginNames.get(i))).thenReturn(i);
            when(originTable.getDataType(standardOriginNames.get(i))).thenReturn(standardOriginTypes.get(i));
            when(originTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardOriginTypes.get(i)));
        }

        for (int i = 0; i< standardTargetNames.size(); i++) {
            when(targetTable.getColumnNames(false)).thenReturn(standardTargetNames);
            when(targetTable.indexOf(standardTargetNames.get(i))).thenReturn(i);
            when(targetTable.getDataType(standardTargetNames.get(i))).thenReturn(standardTargetTypes.get(i));
            when(targetTable.getBindClass(i)).thenReturn(CqlData.getBindClass(standardTargetTypes.get(i)));
        }
    }

    @Test
    public void smokeTest_loadProperties() {
        boolean loaded = feature.loadProperties(propertyHelper);

        assertAll(
                () -> assertTrue(loaded, "properties are loaded and valid"),
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(standardMapColumnName, feature.getOriginColumnName(), "origin name"),
                () -> assertEquals(standardKeyColumnName, feature.getKeyColumnName(), "key name"),
                () -> assertEquals(standardValueColumnName, feature.getValueColumnName(), "value name")
        );
    }


    @Test
    public void smokeTest_initializeAndValidate() {
        feature.loadProperties(propertyHelper);
        boolean valid = feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertTrue(valid, "configuration is valid"),
                () -> assertEquals(standardOriginNames.indexOf(standardMapColumnName), feature.getOriginColumnIndex(), "origin index"),
                () -> assertEquals(standardTargetNames.indexOf(standardKeyColumnName), feature.getKeyColumnIndex(), "key index"),
                () -> assertEquals(standardTargetNames.indexOf(standardValueColumnName), feature.getValueColumnIndex(), "value index")
        );
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


//    @Test
//    public void smokeTest_alterProperties() {
//        setValidSparkConf();
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
//
//        assertAll(
//                () -> assertTrue(feature.isEnabled(), "isEnabled"),
//                () -> assertEquals(Arrays.asList("key","map_key"), ColumnsKeysTypes.getTargetPKNames(helper), "TARGET_PRIMARY_KEY"),
//                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0")), ColumnsKeysTypes.getTargetPKTypes(helper), "TARGET_PRIMARY_KEY_TYPES"),
//                () -> assertEquals(Arrays.asList("key","val","map_key","map_val"), ColumnsKeysTypes.getTargetColumnNames(helper), "TARGET_COLUMN_NAMES"),
//                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("1"),new MigrateDataType("0"),new MigrateDataType("3")), ColumnsKeysTypes.getTargetColumnTypes(helper), "TARGET_COLUMN_TYPES")
//                );
//    }
//
//    @Test
//    public void smokeCQL() {
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
//        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
//        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
//        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
//        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
//        sparkConf.set(KnownProperties.TARGET_CONNECT_HOST, "localhost");
//        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");
//
//        sparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col");
//        sparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
//        sparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_val");
//
//        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key");
//
//        helper.initializeSparkConf(sparkConf);
//        CqlHelper cqlHelper = new CqlHelper();
//        cqlHelper.initialize();
//
//        String originSelect = "SELECT key,val,map_col FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
//        String originSelectByPK = "SELECT key,val,map_col FROM origin.tab1 WHERE key=?";
//        String targetInsert = "INSERT INTO target.tab1 (key,val,map_key,map_val) VALUES (?,?,?,?)";
//        String targetUpdate = "UPDATE target.tab1 SET val=?,map_val=? WHERE key=? AND map_key=?";
//        String targetSelect = "SELECT key,val,map_key,map_val FROM target.tab1 WHERE key=? AND map_key=?";
//
//        assertAll(
//                () -> assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement(null).getCQL().replaceAll("\\s+"," ")),
//                () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement(null).getCQL().replaceAll("\\s+"," ")),
//                () -> assertEquals(targetInsert, cqlHelper.getTargetInsertStatement(null).getCQL().replaceAll("\\s+"," ")),
//                () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement(null).getCQL().replaceAll("\\s+"," ")),
//                () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement(null).getCQL().replaceAll("\\s+"," "))
//        );
//    }
//
//    @Test
//    public void smokeTest_disabled() {
//        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
//        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
//        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
//
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
//    }
//
//    @Test
//    public void value_isOnPK() {
//        // This would be an admittedly strange situation, but it could be valid...
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
//        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key,map_val");
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
//
//        assertAll(
//                () -> assertTrue(feature.isEnabled()),
//                () -> assertEquals(Arrays.asList("key","map_key","map_val"), ColumnsKeysTypes.getTargetPKNames(helper), "TARGET_PRIMARY_KEY"),
//                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0"),new MigrateDataType("3")), ColumnsKeysTypes.getTargetPKTypes(helper), "TARGET_PRIMARY_KEY_TYPES")
//        );
//    }
//
//    @Test
//    public void alterProperties_TargetColumnsSet() {
//        // The configuration should not allow these to be set, but testing to handle a future where they are set
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.TARGET_COLUMN_NAMES, "key,val,map_key,map_val");
//        validSparkConf.set(KnownProperties.TARGET_COLUMN_TYPES, "4,1,0,3");
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
//        assertAll(
//                () -> assertTrue(feature.isEnabled()),
//                () -> assertEquals(Arrays.asList("key","val","map_key","map_val"), helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES), "TARGET_COLUMN_NAMES"),
//                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("1"),new MigrateDataType("0"),new MigrateDataType("3")), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES), "TARGET_COLUMN_TYPES"),
//                () -> assertEquals(helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES).size(), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES).size(), "sizes match")
//        );
//    }
//
//
//    @Test
//    public void invalidConfig_mapType() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,6%0");
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
//    }
//
//    @Test
//    public void invalidConfig_mismatchColumn() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col_not_on_list");
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
//    }
//
//    @Test
//    public void invalidConfig_emptyName() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "");
//        helper.initializeSparkConf(validSparkConf);
//        cqlHelper.initialize();
//        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
//    }
}
