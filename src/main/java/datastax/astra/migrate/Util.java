package datastax.astra.migrate;

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

}
