package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.NoSuchElementException;

public class Util {

    public static String getSparkProp(SparkConf sc, String prop) {
        try {
            return sc.get(prop);
        } catch (NoSuchElementException nse) {
            String newProp = prop.replace("origin", "source").replace("target", "destination");
            return sc.get(newProp);
        }
    }

    public static String getSparkPropOr(SparkConf sc, String prop, String defaultVal) {
        try {
            return sc.get(prop);
        } catch (NoSuchElementException nse) {
            String newProp = prop.replace("origin", "source").replace("target", "destination");
            return sc.get(newProp, defaultVal);
        }
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
