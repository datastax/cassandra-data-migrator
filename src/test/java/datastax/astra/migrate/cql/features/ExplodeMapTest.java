package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ExplodeMapTest {

    PropertyHelper helper;
    SparkConf validSparkConf;
    Feature feature;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
        validSparkConf = new SparkConf();
        feature = FeatureFactory.getFeature(Featureset.EXPLODE_MAP);
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
    }

    private void setValidSparkConf() {
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");

        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col");
        validSparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
        validSparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_val");

        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key");
    }

    @Test
    public void smokeTest_initialize() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals("map_col", feature.getAsString(ExplodeMap.Property.MAP_COLUMN_NAME), "MAP_COLUMN_NAME"),
                () -> assertEquals("map_key", feature.getAsString(ExplodeMap.Property.KEY_COLUMN_NAME), "KEY_COLUMN_NAME"),
                () -> assertEquals("map_val", feature.getAsString(ExplodeMap.Property.VALUE_COLUMN_NAME), "VALUE_COLUMN_NAME"),
                () -> assertEquals(2, feature.getNumber(ExplodeMap.Property.MAP_COLUMN_INDEX), "MAP_COLUMN_INDEX"),
                () -> assertEquals(new MigrateDataType("0"), feature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE), "KEY_COLUMN_TYPE"),
                () -> assertEquals(new MigrateDataType("3"), feature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE), "VALUE_COLUMN_TYPE")
        );
    }

    @Test
    public void smokeTest_alterProperties() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        feature.alterProperties(helper);
        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(Arrays.asList("key","map_key"), helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY), "TARGET_PRIMARY_KEY"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0")), helper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES), "TARGET_PRIMARY_KEY_TYPES"),
                () -> assertEquals(helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY).size(), helper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES).size(), "sizes match"),
                () -> assertEquals(Arrays.asList("key","val","map_key","map_val"), helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES), "TARGET_COLUMN_NAMES"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("1"),new MigrateDataType("0"),new MigrateDataType("3")), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES), "TARGET_COLUMN_TYPES"),
                () -> assertEquals(helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES).size(), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES).size(), "sizes match")
                );
    }

    @Test
    public void smokeCQL() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
        sparkConf.set(KnownProperties.TARGET_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        sparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col");
        sparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
        sparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_val");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,val,map_col FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String targetInsert = "INSERT INTO target.tab1 (key,val,map_key,map_val) VALUES (?,?,?,?)";
        String targetSelect = "SELECT key,val,map_key,map_val FROM target.tab1 WHERE key=? AND map_key=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getCql(CqlHelper.CQL.ORIGIN_SELECT).replaceAll("\\s+"," ")),
                () -> assertEquals(targetInsert, cqlHelper.getCql(CqlHelper.CQL.TARGET_INSERT).replaceAll("\\s+"," ")),
                () -> assertEquals(targetSelect, cqlHelper.getCql(CqlHelper.CQL.TARGET_SELECT_ORIGIN_BY_PK).replaceAll("\\s+"," "))
        );
    }

    @Test
    public void smokeTest_disabled() {
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");

        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void value_isOnPK() {
        // This would be an admittedly strange situation, but it could be valid...
        setValidSparkConf();
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key,map_val");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        feature.alterProperties(helper);
        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(Arrays.asList("key","map_key","map_val"), helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY), "TARGET_PRIMARY_KEY"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0"),new MigrateDataType("3")), helper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES), "TARGET_PRIMARY_KEY_TYPES")
        );
    }

    @Test
    public void alterProperties_TargetColumnsSet() {
        // The configuration should not allow these to be set, but testing to handle a future where they are set
        setValidSparkConf();
        validSparkConf.set(KnownProperties.TARGET_COLUMN_NAMES, "key,val,map_key,map_val");
        validSparkConf.set(KnownProperties.TARGET_COLUMN_TYPES, "4,1,0,3");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        feature.alterProperties(helper);
        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(Arrays.asList("key","val","map_key","map_val"), helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES), "TARGET_COLUMN_NAMES"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("1"),new MigrateDataType("0"),new MigrateDataType("3")), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES), "TARGET_COLUMN_TYPES"),
                () -> assertEquals(helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES).size(), helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES).size(), "sizes match")
        );
    }


    @Test
    public void invalidConfig_mapType() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,6%0");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void invalidConfig_mismatchColumn() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col_not_on_list");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void invalidConfig_emptyName() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }



//
//    @Test
//    public void smokeCQL() {
//        SparkConf sparkConf = new SparkConf();
//        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
//        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
//        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val");
//        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
//        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "0,0");
//        sparkConf.set(KnownProperties.TARGET_CONNECT_HOST, "localhost");
//        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");
//
//        sparkConf.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
//        sparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");
//        sparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abcd',1234");
//        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "const1,key");
//        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY_TYPES, "0,0");
//
//        helper.initializeSparkConf(sparkConf);
//        CqlHelper cqlHelper = new CqlHelper();
//        cqlHelper.initialize();
//
//        String originSelect = "SELECT key,val FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
//        String targetInsert = "INSERT INTO target.tab1 (key,val,const1,const2) VALUES (?,?,'abcd',1234)";
//        String targetSelect = "SELECT key,val FROM target.tab1 WHERE key=? AND const1='abcd'";
//
//        assertAll(
//                () -> assertEquals(originSelect, cqlHelper.getCql(CqlHelper.CQL.ORIGIN_SELECT).replaceAll("\\s+"," ")),
//                () -> assertEquals(targetInsert, cqlHelper.getCql(CqlHelper.CQL.TARGET_INSERT).replaceAll("\\s+"," ")),
//                () -> assertEquals(targetSelect, cqlHelper.getCql(CqlHelper.CQL.TARGET_SELECT_ORIGIN_BY_PK).replaceAll("\\s+"," "))
//        );
//    }
//
//    @Test
//    public void test_missingColumnNames() {
//        setValidSparkConf();
//        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_NAMES);
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_missingColumnValues() {
//        setValidSparkConf();
//        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_VALUES);
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_missingColumnTypes() {
//        setValidSparkConf();
//        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_TYPES);
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_mismatch_ValueCount() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abcd'");
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_mismatch_TypeCount() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "1");
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_wrongValueDelimiter() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, "\\|");
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_emptyDelimiter() {
//        setValidSparkConf();
//        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, "");
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertFalse(feature.isEnabled());
//    }
//
//    @Test
//    public void test_missingPrimaryKeyNames() {
//        setValidSparkConf();
//        validSparkConf.remove(KnownProperties.TARGET_PRIMARY_KEY);
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertTrue(feature.isEnabled());
//    }
//
//    @Test
//    public void test_missingPrimaryKeyTypes() {
//        setValidSparkConf();
//        validSparkConf.remove(KnownProperties.TARGET_PRIMARY_KEY_TYPES);
//        helper.initializeSparkConf(validSparkConf);
//        feature.initialize(helper);
//        assertTrue(feature.isEnabled());
//    }

}
