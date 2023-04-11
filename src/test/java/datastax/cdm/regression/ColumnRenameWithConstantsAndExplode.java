package datastax.cdm.regression;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/*
   This test addresses CDM-34 which needs to migrate from:
      CREATE TABLE "CUSTOMER"."Index" (
          "parameter-name" text PRIMARY KEY,
          "parameter-value" map<text, text>)

   to:
      CREATE TABLE astra.indextable (
          customer text,
          parameter_name text,
          id text,
          value text,
          PRIMARY KEY ((customer, parameter_name), id))

   1. Table to be renamed from Index (mixed case) to lowercase indextable
   2. customer column to be hard-coded to value 'CUSTOMER' (corresponds with keyspace name)
   3. "parameter-name" column to be renamed to lowercase parameter_name with underscore
   4. "parameter-value" map to be exploded into two columns (and multiple rows): id and value

 */
public class ColumnRenameWithConstantsAndExplode {

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
        validSparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "\"CUSTOMER\".\"Index\"");
        validSparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "astra.indextable");

        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "\"parameter-name\",\"parameter-value\"");
        validSparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "0,5%0%0");
        validSparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "\"parameter-name\"");

        validSparkConf.set(KnownProperties.TARGET_COLUMN_NAMES, "customer,parameter_name,id,value");
        validSparkConf.set(KnownProperties.TARGET_COLUMN_NAMES_TO_ORIGIN, "parameter_name:\"parameter-name\"");

        validSparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "customer,parameter_name,id");

        validSparkConf.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "\"parameter-value\"");
        validSparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "id");
        validSparkConf.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "value");

        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_NAMES, "customer");
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0");
        validSparkConf.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'CUSTOMER'");
    }

    @Test
    public void smokeCQL() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        CqlHelper cqlHelper = new CqlHelper();
        cqlHelper.initialize();

        String originSelect = "SELECT \"parameter-name\",\"parameter-value\" FROM \"CUSTOMER\".\"Index\" WHERE TOKEN(\"parameter-name\") >= ? AND TOKEN(\"parameter-name\") <= ? ALLOW FILTERING";
        String originSelectByPK = "SELECT \"parameter-name\",\"parameter-value\" FROM \"CUSTOMER\".\"Index\" WHERE \"parameter-name\"=?";
        String targetInsert = "INSERT INTO astra.indextable (customer,parameter_name,id,value,customer) VALUES (?,?,?,?,'CUSTOMER')";
        String targetUpdate = "UPDATE astra.indextable SET value=? WHERE customer='CUSTOMER' AND parameter_name=? AND id=?";
        String targetSelect = "SELECT customer,parameter_name,id,value FROM astra.indextable WHERE customer='CUSTOMER' AND parameter_name=? AND id=?";

        assertAll(
                () -> assertEquals(originSelect, cqlHelper.getOriginSelectByPartitionRangeStatement().getCQL().replaceAll("\\s+"," "), "originSelect"),
                () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement().getCQL().replaceAll("\\s+"," "), "originSelectByPK"),
                () -> assertEquals(targetInsert, cqlHelper.getTargetInsertStatement().getCQL().replaceAll("\\s+"," "), "targetInsert"),
                () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement().getCQL().replaceAll("\\s+"," "), "targetUpdate"),
                () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement().getCQL().replaceAll("\\s+"," "), "targetSelect")
        );
    }

}
