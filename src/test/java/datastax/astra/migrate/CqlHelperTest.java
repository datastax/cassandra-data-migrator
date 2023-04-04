package datastax.astra.migrate;

import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.cql.features.Featureset;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CqlHelperTest {

    SparkConf sparkConf;
    PropertyHelper propertyHelper;
    CqlHelper cqlHelper;

    @BeforeEach
    public void setup() {
        sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,val");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "0,0");
        sparkConf.set(KnownProperties.TARGET_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");
        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");

        propertyHelper = PropertyHelper.getInstance();
        cqlHelper = new CqlHelper();
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    public void smokeTest() {
        propertyHelper.initializeSparkConf(sparkConf);
        cqlHelper.initialize();

        String originSelect = "SELECT key,val FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,val FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,val) VALUES (?,?)";
        String targetUpdate = "UPDATE target.tab1 SET val=? WHERE key=?";
        String targetSelect = "SELECT key,val FROM target.tab1 WHERE key=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );

    }

    @Test
    public void featureHelper_disabledWhenNull() {
        propertyHelper.initializeSparkConf(sparkConf);
        cqlHelper.initialize();
        assertFalse(cqlHelper.isFeatureEnabled(Featureset.TEST_UNIMPLEMENTED_FEATURE));
    }
}
