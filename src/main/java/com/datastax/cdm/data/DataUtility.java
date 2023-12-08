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
package com.datastax.cdm.data;

import com.datastax.cdm.schema.CqlTable;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataUtility {
    public static final Logger logger = LoggerFactory.getLogger(CqlConversion.class);

    public static boolean diff(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) {
            return false;
        } else if (obj1 == null && obj2 != null) {
            return true;
        } else if (obj1 != null && obj2 == null) {
            return true;
        }

        return !obj1.equals(obj2);
    }

    public static List<Object> extractObjectsFromCollection(Object collection) {
        List<Object> objects = new ArrayList<>();
        if (collection instanceof List) {
            objects.addAll((List) collection);
        } else if (collection instanceof Set) {
            objects.addAll((Set) collection);
        } else if (collection instanceof Map) {
            objects.addAll(((Map) collection).entrySet());
        }
        return objects;
    }

    public static Map<String,String> getThisToThatColumnNameMap(IPropertyHelper propertyHelper, CqlTable thisCqlTable, CqlTable thatCqlTable) {
        // Property ORIGIN_COLUMN_NAMES_TO_TARGET is a list of origin column name to target column name mappings
        // Use that as the starting point for the return map
        List<String> originColumnNames = thisCqlTable.isOrigin() ? thisCqlTable.getColumnNames(false) : thatCqlTable.getColumnNames(false);
        List<String> targetColumnNames = thisCqlTable.isOrigin() ? thatCqlTable.getColumnNames(false) : thisCqlTable.getColumnNames(false);

        if (logger.isDebugEnabled()) {
            logger.debug("originColumnNames: " + originColumnNames);
            logger.debug("targetColumnNames: " + targetColumnNames);
        }

        List<String> originColumnNamesToTarget = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET);
        Map<String,String> originToTargetNameMap = new HashMap<>();
        if (null!=originColumnNamesToTarget && !originColumnNamesToTarget.isEmpty()) {
            for (String pair : originColumnNamesToTarget) {
                String[] parts = pair.split(":");
                if (parts.length != 2 || null == parts[0] || null == parts[1] ||
                        parts[0].isEmpty() || parts[1].isEmpty()) {
                    throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET + " pair is mis-configured, either a missing ':' separator or one/both sides are empty: " + pair);
                }
                String originColumnName = CqlTable.unFormatName(parts[0]);
                String targetColumnName = CqlTable.unFormatName(parts[1]);

                if (originColumnNames.contains(originColumnName) && targetColumnNames.contains(targetColumnName)) {
                    originToTargetNameMap.put(originColumnName, targetColumnName);
                }
                else {
                    throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET + " one or both columns are not found on the table: " + pair);
                }
            }
        }

        // Next, add any origin column names that are not on the map, and add them if there is a matching target column name
        for (String originColumnName : originColumnNames) {
            if (!originToTargetNameMap.containsKey(originColumnName)) {
                if (targetColumnNames.contains(originColumnName)) {
                    originToTargetNameMap.put(originColumnName, originColumnName);
                }
            }
        }

        // If thisCqlTable is the origin, then return the map as is
        // If thisCqlTable is the target, then reverse the map, and then if there are any
        // target column names that are not on the map, and add them if there is a matching
        // origin column name
        if (thisCqlTable.isOrigin()) {
            return originToTargetNameMap;
        } else {
            Map<String,String> targetToOriginNameMap = new HashMap<>();
            for (String originColumnName : originToTargetNameMap.keySet()) {
                String targetColumnName = originToTargetNameMap.get(originColumnName);
                targetToOriginNameMap.put(targetColumnName, originColumnName);
            }
            for (String targetColumnName : targetColumnNames) {
                if (!targetToOriginNameMap.containsKey(targetColumnName)) {
                    if (originColumnNames.contains(targetColumnName)) {
                        targetToOriginNameMap.put(targetColumnName, targetColumnName);
                    }
                }
            }
            return targetToOriginNameMap;
        }
    }

    public static String getMyClassMethodLine(Exception e) {
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        StackTraceElement targetStackTraceElement = null;
        for (StackTraceElement element : stackTraceElements) {
            if (element.getClassName().startsWith("com.datastax.cdm")) {
                targetStackTraceElement = element;
                break;
            }
        }
        String className = targetStackTraceElement.getClassName();
        String methodName = targetStackTraceElement.getMethodName();
        int lineNumber = targetStackTraceElement.getLineNumber();

        return className + "." + methodName + ":" + lineNumber;
    }
}
