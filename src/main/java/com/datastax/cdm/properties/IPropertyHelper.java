/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.properties;

import java.util.List;

import org.apache.spark.SparkConf;

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

    boolean meetsMinimum(String valueName, Long testValue, Long minimumValue);
}
