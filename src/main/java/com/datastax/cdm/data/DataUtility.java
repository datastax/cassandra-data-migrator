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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;

public class DataUtility {
    public static final Logger logger = LoggerFactory.getLogger(DataUtility.class.getName());

    protected static final String SCB_FILE_NAME = "_temp_cdm_scb_do_not_touch.zip";
    protected static final int SCB_DELETE_DELAY = 5;

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

    public static Map<String, String> getThisToThatColumnNameMap(IPropertyHelper propertyHelper, CqlTable thisCqlTable,
            CqlTable thatCqlTable) {
        // Property ORIGIN_COLUMN_NAMES_TO_TARGET is a list of origin column name to target column name mappings
        // Use that as the starting point for the return map
        List<String> originColumnNames = thisCqlTable.isOrigin() ? thisCqlTable.getColumnNames(false)
                : thatCqlTable.getColumnNames(false);
        List<String> targetColumnNames = thisCqlTable.isOrigin() ? thatCqlTable.getColumnNames(false)
                : thisCqlTable.getColumnNames(false);

        if (logger.isDebugEnabled()) {
            logger.debug("originColumnNames: " + originColumnNames);
            logger.debug("targetColumnNames: " + targetColumnNames);
        }

        List<String> originColumnNamesToTarget = propertyHelper
                .getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET);
        Map<String, String> originToTargetNameMap = new HashMap<>();
        if (null != originColumnNamesToTarget && !originColumnNamesToTarget.isEmpty()) {
            for (String pair : originColumnNamesToTarget) {
                String[] parts = pair.split(":");
                if (parts.length != 2 || null == parts[0] || null == parts[1] || parts[0].isEmpty()
                        || parts[1].isEmpty()) {
                    throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET
                            + " pair is mis-configured, either a missing ':' separator or one/both sides are empty: "
                            + pair);
                }
                String originColumnName = CqlTable.unFormatName(parts[0]);
                String targetColumnName = CqlTable.unFormatName(parts[1]);

                if (originColumnNames.contains(originColumnName) && targetColumnNames.contains(targetColumnName)) {
                    originToTargetNameMap.put(originColumnName, targetColumnName);
                } else {
                    throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET
                            + " one or both columns are not found on the table: " + pair);
                }
            }
        }

        // Next, add any origin column names that are not on the map, and add them if there is a matching target column
        // name
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
            Map<String, String> targetToOriginNameMap = new HashMap<>();
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
        if (null == targetStackTraceElement && null != stackTraceElements && stackTraceElements.length > 0) {
            targetStackTraceElement = stackTraceElements[0];
        }
        if (null != targetStackTraceElement) {
            String className = targetStackTraceElement.getClassName();
            String methodName = targetStackTraceElement.getMethodName();
            int lineNumber = targetStackTraceElement.getLineNumber();
            return className + "." + methodName + ":" + lineNumber;
        }

        return "Unknown";
    }

    public static void deleteGeneratedSCB(long runId, int waitSeconds) {
        CompletableFuture.runAsync(() -> {
            try {
                File originFile = new File(PKFactory.Side.ORIGIN + "_" + Long.toString(runId) + SCB_FILE_NAME);
                File targetFile = new File(PKFactory.Side.TARGET + "_" + Long.toString(runId) + SCB_FILE_NAME);

                if (originFile.exists() || targetFile.exists()) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(waitSeconds));
                    if (originFile.exists())
                        originFile.delete();
                    if (targetFile.exists())
                        targetFile.delete();
                }
            } catch (Exception e) {
                logger.error("Unable to delete generated SCB files: {}", e.getMessage());
            }
        });
    }

    public static void deleteGeneratedSCB(long runId) {
        deleteGeneratedSCB(runId, SCB_DELETE_DELAY);
    }

    public static File generateSCB(String host, String port, String trustStorePassword, String trustStorePath,
            String keyStorePassword, String keyStorePath, PKFactory.Side side, long runId) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream("config.json");
        String scbJson = new StringBuilder("{\"host\": \"").append(host).append("\", \"port\": ").append(port)
                .append(", \"keyStoreLocation\": \"./identity.jks\", \"keyStorePassword\": \"").append(keyStorePassword)
                .append("\", \"trustStoreLocation\": \"./trustStore.jks\", \"trustStorePassword\": \"")
                .append(trustStorePassword).append("\"}").toString();
        fileOutputStream.write(scbJson.getBytes());
        fileOutputStream.close();
        File configFile = new File("config.json");
        FilePathAndNewName configFileWithName = new FilePathAndNewName(configFile, "config.json");
        FilePathAndNewName keyFileWithName = new FilePathAndNewName(new File(keyStorePath), "identity.jks");
        FilePathAndNewName trustFileWithName = new FilePathAndNewName(new File(trustStorePath), "trustStore.jks");
        File zipFile = zip(Arrays.asList(configFileWithName, keyFileWithName, trustFileWithName),
                side + "_" + Long.toString(runId) + SCB_FILE_NAME);
        configFile.delete();

        return zipFile;
    }

    private static File zip(List<FilePathAndNewName> files, String filename) {
        File zipfile = new File(filename);
        byte[] buf = new byte[1024];
        try {
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipfile));
            for (int i = 0; i < files.size(); i++) {
                out.putNextEntry(new ZipEntry(files.get(i).name));
                FileInputStream in = new FileInputStream(files.get(i).file.getCanonicalPath());
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                out.closeEntry();
                in.close();
            }
            out.close();

            return zipfile;
        } catch (IOException ex) {
            logger.error("Unable to write out zip file: {}. Got exception: {}", filename, ex.getMessage());
        }
        return null;
    }

    static class FilePathAndNewName {
        File file;
        String name;

        public FilePathAndNewName(File file, String name) {
            this.file = file;
            this.name = name;
        }
    }
}
