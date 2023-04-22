package com.datastax.cdm.properties;

import org.apache.spark.SparkConf;
import java.util.List;

public interface IPropertyHelper {

    void initializeSparkConf(SparkConf sc);

    Object setProperty(String propertyName, Object propertyValue);

    String getString(String propertyName);

    List<String> getStringList(String propertyName);

    Number getNumber(String propertyName);

    Integer getInteger(String propertyName);

    Long getLong(String propertyName);

    List<Number> getNumberList(String propertyName);

    List<Integer> getIntegerList(String propertyName);

    Boolean getBoolean(String propertyName);

    String getAsString(String propertyName);

    boolean isSparkConfFullyLoaded();

    boolean meetsMinimum(String valueName, Integer testValue, Integer minimumValue);
}
