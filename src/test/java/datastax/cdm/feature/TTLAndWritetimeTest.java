package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TTLAndWritetimeTest {

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
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,col1,col2,col3,col4");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,2,4,3");
        validSparkConf.set(KnownProperties.ORIGIN_TTL_INDEXES, "1,2");
        validSparkConf.set(KnownProperties.ORIGIN_WRITETIME_INDEXES, "2,3");

        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
    }

    @Test
    public void smokeTest_initialize() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        assertAll(
                () -> assertEquals(Arrays.asList(1,2), helper.getIntegerList(KnownProperties.ORIGIN_TTL_INDEXES), "ORIGIN_TTL_COLS"),
                () -> assertEquals(Arrays.asList(2,3), helper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_INDEXES), "ORIGIN_WRITETIME_COLS")
        );
    }

    @Test
    public void smokeCQL_ttl() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,t_col1,tw_col2,w_col3,col4");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,2,4,3");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_TTL_INDEXES, "1,2");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "TTL(t_col1) as ttl_t_col1,TTL(tw_col2) as ttl_tw_col2 "+
                "FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "TTL(t_col1) as ttl_t_col1,TTL(tw_col2) as ttl_tw_col2 "+
                "FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,t_col1,tw_col2,w_col3,col4) VALUES (?,?,?,?,?) USING TTL ?";
        String targetUpdate = "UPDATE target.tab1 USING TTL ? SET t_col1=?,tw_col2=?,w_col3=?,col4=? WHERE key=?";
        String targetSelect = "SELECT key,t_col1,tw_col2,w_col3,col4 FROM target.tab1 WHERE key=?";

        assertAll(
                () -> Assertions.assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );
    }

    @Test
    public void smokeCQL_writetime() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,t_col1,tw_col2,w_col3,col4");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,2,4,3");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_WRITETIME_INDEXES, "2,3");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "WRITETIME(tw_col2) as writetime_tw_col2,WRITETIME(w_col3) as writetime_w_col3 "+
                "FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "WRITETIME(tw_col2) as writetime_tw_col2,WRITETIME(w_col3) as writetime_w_col3 "+
                "FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,t_col1,tw_col2,w_col3,col4) VALUES (?,?,?,?,?) USING TIMESTAMP ?";
        String targetUpdate = "UPDATE target.tab1 USING TIMESTAMP ? SET t_col1=?,tw_col2=?,w_col3=?,col4=? WHERE key=?";
        String targetSelect = "SELECT key,t_col1,tw_col2,w_col3,col4 FROM target.tab1 WHERE key=?";

        assertAll(
                () -> Assertions.assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );
    }

    @Test
    public void smokeCQL_both() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,t_col1,tw_col2,w_col3,col4");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,2,4,3");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_TTL_INDEXES, "1,2");
        sparkConf.set(KnownProperties.ORIGIN_WRITETIME_INDEXES, "2,3");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "TTL(t_col1) as ttl_t_col1,TTL(tw_col2) as ttl_tw_col2,"+
                "WRITETIME(tw_col2) as writetime_tw_col2,WRITETIME(w_col3) as writetime_w_col3 "+
                "FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,t_col1,tw_col2,w_col3,col4,"+
                "TTL(t_col1) as ttl_t_col1,TTL(tw_col2) as ttl_tw_col2,"+
                "WRITETIME(tw_col2) as writetime_tw_col2,WRITETIME(w_col3) as writetime_w_col3 "+
                "FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,t_col1,tw_col2,w_col3,col4) VALUES (?,?,?,?,?) USING TTL ? AND TIMESTAMP ?";
        String targetUpdate = "UPDATE target.tab1 USING TTL ? AND TIMESTAMP ? SET t_col1=?,tw_col2=?,w_col3=?,col4=? WHERE key=?";
        String targetSelect = "SELECT key,t_col1,tw_col2,w_col3,col4 FROM target.tab1 WHERE key=?";

        assertAll(
                () -> Assertions.assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );
    }

    @Test
    public void dashesAndQuotesInColumnNames() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
        sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,\"t-col1\",\"tw-col2\",\"w-col3\",col4");
        sparkConf.set(KnownProperties.TARGET_COLUMN_NAMES, "key,t_col1,tw_col2,w_col3,col4");
        sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,1,2,4,3");
        sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
        sparkConf.set(KnownProperties.ORIGIN_TTL_INDEXES, "1,2");
        sparkConf.set(KnownProperties.ORIGIN_WRITETIME_INDEXES, "2,3");

        sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
        sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");

        helper.initializeSparkConf(sparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT key,\"t-col1\",\"tw-col2\",\"w-col3\",col4,"+
                "TTL(\"t-col1\") as ttl_t_col1,TTL(\"tw-col2\") as ttl_tw_col2,"+
                "WRITETIME(\"tw-col2\") as writetime_tw_col2,WRITETIME(\"w-col3\") as writetime_w_col3 "+
                "FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT key,\"t-col1\",\"tw-col2\",\"w-col3\",col4,"+
                "TTL(\"t-col1\") as ttl_t_col1,TTL(\"tw-col2\") as ttl_tw_col2,"+
                "WRITETIME(\"tw-col2\") as writetime_tw_col2,WRITETIME(\"w-col3\") as writetime_w_col3 "+
                "FROM origin.tab1 WHERE key=?";
        String targetInsert = "INSERT INTO target.tab1 (key,t_col1,tw_col2,w_col3,col4) VALUES (?,?,?,?,?) USING TTL ? AND TIMESTAMP ?";
        String targetUpdate = "UPDATE target.tab1 USING TTL ? AND TIMESTAMP ? SET t_col1=?,tw_col2=?,w_col3=?,col4=? WHERE key=?";
        String targetSelect = "SELECT key,t_col1,tw_col2,w_col3,col4 FROM target.tab1 WHERE key=?";

        assertAll(
                () -> Assertions.assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," ")),
                () -> Assertions.assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "))
        );
    }

}
