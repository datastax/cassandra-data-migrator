package datastax.cdm.feature;

import datastax.cdm.data.PKFactory;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ExplodeMapTest {

    PropertyHelper helper;
    CqlHelper cqlHelper;
    SparkConf validSparkConf;
    Feature feature;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
        cqlHelper = new CqlHelper();
        validSparkConf = new SparkConf();
        feature = FeatureFactory.getFeature(Featureset.EXPLODE_MAP);
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
        feature = null;
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
        cqlHelper.initialize();
        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);

        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals("map_col", feature.getAsString(ExplodeMap.Property.MAP_COLUMN_NAME), "MAP_COLUMN_NAME"),
                () -> assertEquals("map_key", feature.getAsString(ExplodeMap.Property.KEY_COLUMN_NAME), "KEY_COLUMN_NAME"),
                () -> assertEquals("map_val", feature.getAsString(ExplodeMap.Property.VALUE_COLUMN_NAME), "VALUE_COLUMN_NAME"),
                () -> assertEquals(2, feature.getNumber(ExplodeMap.Property.MAP_COLUMN_INDEX), "MAP_COLUMN_INDEX"),
                () -> assertEquals(new MigrateDataType("5%0%3"), feature.getMigrateDataType(ExplodeMap.Property.MAP_COLUMN_TYPE), "MAP_COLUMN_TYPE"),
                () -> assertEquals(new MigrateDataType("0"), feature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE), "KEY_COLUMN_TYPE"),
                () -> assertEquals(new MigrateDataType("3"), feature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE), "VALUE_COLUMN_TYPE")
        );
    }

    @Test
    public void smokeTest_alterProperties() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);

        assertAll(
                () -> assertTrue(feature.isEnabled(), "isEnabled"),
                () -> assertEquals(Arrays.asList("key","map_key"), ColumnsKeysTypes.getTargetPKNames(helper), "TARGET_PRIMARY_KEY"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0")), ColumnsKeysTypes.getTargetPKTypes(helper), "TARGET_PRIMARY_KEY_TYPES"),
                () -> assertEquals(Arrays.asList("key","val","map_key","map_val"), ColumnsKeysTypes.getTargetColumnNames(helper), "TARGET_COLUMN_NAMES"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("1"),new MigrateDataType("0"),new MigrateDataType("3")), ColumnsKeysTypes.getTargetColumnTypes(helper), "TARGET_COLUMN_TYPES")
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
        String originSelectByPK = "SELECT key,val,map_col FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,val,map_key,map_val) VALUES (?,?,?,?)";
        String targetUpdate = "UPDATE target.tab1 SET val=?,map_val=? WHERE key=? AND map_key=?";
        String targetSelect = "SELECT key,val,map_key,map_val FROM target.tab1 WHERE key=? AND map_key=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement(null).getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement(null).getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetInsert, cqlHelper.getTargetInsertStatement(null).getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement(null).getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement(null).getCQL().replaceAll("\\s+"," "))
        );
    }

    @Test
    public void smokeTest_disabled() {
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val,map_col");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");

        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
    }

    @Test
    public void value_isOnPK() {
        // This would be an admittedly strange situation, but it could be valid...
        setValidSparkConf();
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,5%0%3");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key,map_key,map_val");
        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);

        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals(Arrays.asList("key","map_key","map_val"), ColumnsKeysTypes.getTargetPKNames(helper), "TARGET_PRIMARY_KEY"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("4"),new MigrateDataType("0"),new MigrateDataType("3")), ColumnsKeysTypes.getTargetPKTypes(helper), "TARGET_PRIMARY_KEY_TYPES")
        );
    }

    @Test
    public void alterProperties_TargetColumnsSet() {
        // The configuration should not allow these to be set, but testing to handle a future where they are set
        setValidSparkConf();
        validSparkConf.set(KnownProperties.TARGET_COLUMN_NAMES, "key,val,map_key,map_val");
        validSparkConf.set(KnownProperties.TARGET_COLUMN_TYPES, "4,1,0,3");
        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        feature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
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
        cqlHelper.initialize();
        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
    }

    @Test
    public void invalidConfig_mismatchColumn() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map_col_not_on_list");
        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
    }

    @Test
    public void invalidConfig_emptyName() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "");
        helper.initializeSparkConf(validSparkConf);
        cqlHelper.initialize();
        assertNull(cqlHelper.getFeature(Featureset.EXPLODE_MAP));
    }
}
