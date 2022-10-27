package datastax.astra.migrate;

import org.apache.spark.SparkConf;

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

}
