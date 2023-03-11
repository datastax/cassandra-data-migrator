package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.NoSuchElementException;

public class Util {

    public static String getSparkProp(SparkConf sc, String prop) {
        if (!sc.contains(prop)) {
            return sc.get(prop.replace("origin", "source").replace("target", "destination"));
        }
        if (!KnownProperties.isKnown(prop)) {
            throw new IllegalArgumentException("Unknown property: " + prop + "; this is a bug in the code: the property is not configured in KnownProperties.java");
        }
        return PropertyHelper.getInstance(sc).getAsString(prop);
    }

    public static String getSparkPropOr(SparkConf sc, String prop, String defaultVal) {
        if (!sc.contains(prop)) {
            return sc.get(prop.replace("origin", "source").replace("target", "destination"), defaultVal);
        }
        String retVal = getSparkProp(sc,prop);
        if (null == retVal) {
            PropertyHelper.getInstance(sc).setProperty(prop, defaultVal);
            return getSparkProp(sc,prop);
        }
        return retVal;
    }

    public static String getSparkPropOrEmpty(SparkConf sc, String prop) {
        return getSparkPropOr(sc, prop, "");
    }

    public static BufferedReader getfileReader(String fileName) {
        try {
            return new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException("No '" + fileName + "' file found!! Add this file in the current folder & rerun!");
        }
    }

    public static ConsistencyLevel mapToConsistencyLevel(String level) {
        ConsistencyLevel retVal = ConsistencyLevel.LOCAL_QUORUM;
        if (StringUtils.isNotEmpty(level)) {
            switch (level.toUpperCase()) {
                case "ANY":
                    retVal = ConsistencyLevel.ANY;
                    break;
                case "ONE":
                    retVal = ConsistencyLevel.ONE;
                    break;
                case "TWO":
                    retVal = ConsistencyLevel.TWO;
                    break;
                case "THREE":
                    retVal = ConsistencyLevel.THREE;
                    break;
                case "QUORUM":
                    retVal = ConsistencyLevel.QUORUM;
                    break;
                case "LOCAL_ONE":
                    retVal = ConsistencyLevel.LOCAL_ONE;
                    break;
                case "EACH_QUORUM":
                    retVal = ConsistencyLevel.EACH_QUORUM;
                    break;
                case "SERIAL":
                    retVal = ConsistencyLevel.SERIAL;
                    break;
                case "LOCAL_SERIAL":
                    retVal = ConsistencyLevel.LOCAL_SERIAL;
                    break;
                case "ALL":
                    retVal = ConsistencyLevel.ALL;
                    break;
            }
        }

        return retVal;
    }

}
