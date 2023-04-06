package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CounterTest {

    PropertyHelper helper;
    SparkConf validSparkConf;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
        validSparkConf = new SparkConf();
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
    }

    private void setValidSparkConf() {
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,col1,col2");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,2,2");
        validSparkConf.set(KnownProperties.ORIGIN_COUNTER_INDEXES, "1,2");

        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
    }

    @Test
    public void smokeTest_initialize() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        assertAll(
                () -> assertEquals(Arrays.asList(1,2), helper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEXES), "ORIGIN_COUNTER_INDEXES")
        );
    }

    @Test
    public void smokeCQL() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,col1,col2");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,2,2");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_COUNTER_INDEXES, "1,2");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,col1,col2 FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,col1,col2 FROM origin.tab1 WHERE key=?";
        String targetUpdate = "UPDATE target.tab1 SET col1=col1+?,col2=col2+? WHERE key=?";
        String targetSelect = "SELECT key,col1,col2 FROM target.tab1 WHERE key=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );
    }
}
