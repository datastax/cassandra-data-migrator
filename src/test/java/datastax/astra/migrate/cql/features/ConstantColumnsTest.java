package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConstantColumnsTest {

    PropertyHelper helper;
    SparkConf validSparkConf;
    Feature feature;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
        validSparkConf = new SparkConf();
        feature = FeatureFactory.getFeature(Featureset.CONSTANT_COLUMNS);
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
    }

    private void setValidSparkConf() {
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val");
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abcd',1234");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "const1,key");
        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY_TYPES, "0,0");
    }

    @Test
    public void smokeTest() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertAll(
                () -> assertTrue(feature.isEnabled()),
                () -> assertEquals("const1,const2", feature.getAsString(ConstantColumns.Property.COLUMN_NAMES)),
                () -> assertEquals("0,1", feature.getAsString(ConstantColumns.Property.COLUMN_TYPES)),
                () -> assertEquals("'abcd',1234", feature.getAsString(ConstantColumns.Property.COLUMN_VALUES)),
                () -> assertEquals("0", feature.getAsString(ConstantColumns.Property.TARGET_PRIMARY_TYPES_WITHOUT_CONSTANT))
        );
    }

    @Test
    public void smokeCQL() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "0,0");
        sparkConf.set(KnownProperties.TARGET_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        sparkConf.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
        sparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");
        sparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abcd',1234");
        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "const1,key");
        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY_TYPES, "0,0");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,val FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String targetInsert = "INSERT INTO target.tab1 (key,val,const1,const2) VALUES (?,?,'abcd',1234)";
        String targetSelect = "SELECT key,val FROM target.tab1 WHERE const1='abcd' AND key=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getCql(CqlHelper.CQL.ORIGIN_SELECT).replaceAll("\\s+"," ")),
                () -> assertEquals(targetInsert, cqlHelper.getCql(CqlHelper.CQL.TARGET_INSERT).replaceAll("\\s+"," ")),
                () -> assertEquals(targetSelect, cqlHelper.getCql(CqlHelper.CQL.TARGET_SELECT_ORIGIN_BY_PK).replaceAll("\\s+"," "))
        );
    }

    @Test
    public void test_missingColumnNames() {
        setValidSparkConf();
        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_NAMES);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_missingColumnValues() {
        setValidSparkConf();
        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_VALUES);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_missingColumnTypes() {
        setValidSparkConf();
        validSparkConf.remove(KnownProperties.CONSTANT_COLUMN_TYPES);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_mismatch_ValueCount() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abcd'");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_mismatch_TypeCount() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "1");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_wrongValueDelimiter() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, "\\|");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_emptyDelimiter() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, "");
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void test_missingPrimaryKeyNames() {
        setValidSparkConf();
        validSparkConf.remove(KnownProperties.TARGET_PRIMARY_KEY);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertTrue(feature.isEnabled());
    }

    @Test
    public void test_missingPrimaryKeyTypes() {
        setValidSparkConf();
        validSparkConf.remove(KnownProperties.TARGET_PRIMARY_KEY_TYPES);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);
        assertTrue(feature.isEnabled());
    }

}
