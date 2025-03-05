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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KnownProperties {

    public enum PropertyType {
        STRING, NUMBER, BOOLEAN, STRING_LIST, NUMBER_LIST, TEST_UNHANDLED_TYPE
    }

    private static Map<String, PropertyType> types = new HashMap<>();
    private static Map<String, String> defaults = new HashMap<>();
    private static Set<String> required = new HashSet<>();

    // ==========================================================================
    // Common connection parameters
    // ==========================================================================
    public static final String CONNECT_ORIGIN_HOST = "spark.cdm.connect.origin.host";
    public static final String CONNECT_ORIGIN_PORT = "spark.cdm.connect.origin.port";
    public static final String CONNECT_ORIGIN_SCB = "spark.cdm.connect.origin.scb";
    public static final String CONNECT_ORIGIN_USERNAME = "spark.cdm.connect.origin.username";
    public static final String CONNECT_ORIGIN_PASSWORD = "spark.cdm.connect.origin.password";

    public static final String CONNECT_TARGET_HOST = "spark.cdm.connect.target.host";
    public static final String CONNECT_TARGET_PORT = "spark.cdm.connect.target.port";
    public static final String CONNECT_TARGET_SCB = "spark.cdm.connect.target.scb";
    public static final String CONNECT_TARGET_USERNAME = "spark.cdm.connect.target.username";
    public static final String CONNECT_TARGET_PASSWORD = "spark.cdm.connect.target.password";

    static {
        types.put(CONNECT_ORIGIN_HOST, PropertyType.STRING);
        defaults.put(CONNECT_ORIGIN_HOST, "localhost");
        types.put(CONNECT_ORIGIN_PORT, PropertyType.NUMBER);
        defaults.put(CONNECT_ORIGIN_PORT, "9042");
        types.put(CONNECT_ORIGIN_SCB, PropertyType.STRING);
        types.put(CONNECT_ORIGIN_USERNAME, PropertyType.STRING);
        defaults.put(CONNECT_ORIGIN_USERNAME, "cassandra");
        types.put(CONNECT_ORIGIN_PASSWORD, PropertyType.STRING);
        defaults.put(CONNECT_ORIGIN_PASSWORD, "cassandra");

        types.put(CONNECT_TARGET_HOST, PropertyType.STRING);
        defaults.put(CONNECT_TARGET_HOST, "localhost");
        types.put(CONNECT_TARGET_PORT, PropertyType.NUMBER);
        defaults.put(CONNECT_TARGET_PORT, "9042");
        types.put(CONNECT_TARGET_SCB, PropertyType.STRING);
        types.put(CONNECT_TARGET_USERNAME, PropertyType.STRING);
        defaults.put(CONNECT_TARGET_USERNAME, "cassandra");
        types.put(CONNECT_TARGET_PASSWORD, PropertyType.STRING);
        defaults.put(CONNECT_TARGET_PASSWORD, "cassandra");
    }

    // ==========================================================================
    // Properties that describe the origin schema
    // ==========================================================================
    public static final String ORIGIN_KEYSPACE_TABLE = "spark.cdm.schema.origin.keyspaceTable";
    public static final String ORIGIN_TTL_AUTO = "spark.cdm.schema.origin.column.ttl.automatic";
    public static final String ORIGIN_TTL_NAMES = "spark.cdm.schema.origin.column.ttl.names";
    public static final String ORIGIN_WRITETIME_AUTO = "spark.cdm.schema.origin.column.writetime.automatic";
    public static final String ORIGIN_WRITETIME_NAMES = "spark.cdm.schema.origin.column.writetime.names";
    public static final String ALLOW_COLL_FOR_WRITETIME_TTL_CALC = "spark.cdm.schema.ttlwritetime.calc.useCollections";

    public static final String ORIGIN_COLUMN_SKIP = "spark.cdm.schema.origin.column.skip";
    public static final String ORIGIN_COLUMN_NAMES_TO_TARGET = "spark.cdm.schema.origin.column.names.to.target";

    static {
        types.put(ORIGIN_KEYSPACE_TABLE, PropertyType.STRING);
        required.add(ORIGIN_KEYSPACE_TABLE);
        types.put(ORIGIN_TTL_NAMES, PropertyType.STRING_LIST);
        types.put(ORIGIN_TTL_AUTO, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_TTL_AUTO, "true");
        types.put(ORIGIN_WRITETIME_NAMES, PropertyType.STRING_LIST);
        types.put(ORIGIN_WRITETIME_AUTO, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_WRITETIME_AUTO, "true");
        types.put(ALLOW_COLL_FOR_WRITETIME_TTL_CALC, PropertyType.BOOLEAN);
        defaults.put(ALLOW_COLL_FOR_WRITETIME_TTL_CALC, "false");
        types.put(ORIGIN_COLUMN_SKIP, PropertyType.STRING_LIST);
        types.put(ORIGIN_COLUMN_NAMES_TO_TARGET, PropertyType.STRING_LIST);
    }

    // ==========================================================================
    // Properties that describe the target schema
    // ==========================================================================
    public static final String TARGET_KEYSPACE_TABLE = "spark.cdm.schema.target.keyspaceTable";

    static {
        types.put(TARGET_KEYSPACE_TABLE, PropertyType.STRING);
    }

    // ==========================================================================
    // Autocorrection, Performance, and Operations Parameters
    // ==========================================================================
    public static final String AUTOCORRECT_MISSING = "spark.cdm.autocorrect.missing"; // false
    public static final String AUTOCORRECT_MISMATCH = "spark.cdm.autocorrect.mismatch"; // false
    public static final String AUTOCORRECT_MISSING_COUNTER = "spark.cdm.autocorrect.missing.counter"; // false
    public static final String TRACK_RUN = "spark.cdm.trackRun";
    public static final String RUN_ID = "spark.cdm.trackRun.runId";
    public static final String PREV_RUN_ID = "spark.cdm.trackRun.previousRunId";

    public static final String PERF_NUM_PARTS = "spark.cdm.perfops.numParts"; // 5000, was spark.splitSize
    public static final String PERF_BATCH_SIZE = "spark.cdm.perfops.batchSize"; // 5
    public static final String PERF_RATELIMIT_ORIGIN = "spark.cdm.perfops.ratelimit.origin"; // 20000
    public static final String PERF_RATELIMIT_TARGET = "spark.cdm.perfops.ratelimit.target"; // 20000

    public static final String READ_CL = "spark.cdm.perfops.consistency.read";
    public static final String WRITE_CL = "spark.cdm.perfops.consistency.write";
    public static final String PERF_FETCH_SIZE = "spark.cdm.perfops.fetchSizeInRows";

    static {
        types.put(AUTOCORRECT_MISSING, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISSING, "false");
        types.put(AUTOCORRECT_MISMATCH, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISMATCH, "false");
        types.put(AUTOCORRECT_MISSING_COUNTER, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISSING_COUNTER, "false");
        types.put(TRACK_RUN, PropertyType.BOOLEAN);
        defaults.put(TRACK_RUN, "false");
        types.put(RUN_ID, PropertyType.NUMBER);
        defaults.put(RUN_ID, "0");
        types.put(PREV_RUN_ID, PropertyType.NUMBER);
        defaults.put(PREV_RUN_ID, "0");

        types.put(PERF_NUM_PARTS, PropertyType.NUMBER);
        defaults.put(PERF_NUM_PARTS, "5000");
        types.put(PERF_BATCH_SIZE, PropertyType.NUMBER);
        defaults.put(PERF_BATCH_SIZE, "5");
        types.put(PERF_RATELIMIT_ORIGIN, PropertyType.NUMBER);
        defaults.put(PERF_RATELIMIT_ORIGIN, "20000");
        types.put(PERF_RATELIMIT_TARGET, PropertyType.NUMBER);
        defaults.put(PERF_RATELIMIT_TARGET, "20000");

        types.put(READ_CL, PropertyType.STRING);
        defaults.put(READ_CL, "LOCAL_QUORUM");
        types.put(WRITE_CL, PropertyType.STRING);
        defaults.put(WRITE_CL, "LOCAL_QUORUM");
        types.put(PERF_FETCH_SIZE, PropertyType.NUMBER);
        defaults.put(PERF_FETCH_SIZE, "1000");
    }

    // ==========================================================================
    // Transformations
    // ==========================================================================
    public static final String TRANSFORM_REPLACE_MISSING_TS = "spark.cdm.transform.missing.key.ts.replace.value";
    public static final String TRANSFORM_CUSTOM_WRITETIME = "spark.cdm.transform.custom.writetime";
    public static final String TRANSFORM_CUSTOM_WRITETIME_INCREMENT = "spark.cdm.transform.custom.writetime.incrementBy";
    public static final String TRANSFORM_CUSTOM_TTL = "spark.cdm.transform.custom.ttl";
    public static final String TRANSFORM_CODECS = "spark.cdm.transform.codecs";
    public static final String TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT = "spark.cdm.transform.codecs.timestamp.string.format";
    public static final String TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE = "spark.cdm.transform.codecs.timestamp.string.zone";
    public static final String TRANSFORM_MAP_REMOVE_KEY_WITH_NO_VALUE = "spark.cdm.transform.map.remove.null.value";

    static {
        types.put(TRANSFORM_REPLACE_MISSING_TS, PropertyType.NUMBER);
        types.put(TRANSFORM_CUSTOM_WRITETIME, PropertyType.NUMBER);
        defaults.put(TRANSFORM_CUSTOM_WRITETIME, "0");
        types.put(TRANSFORM_CUSTOM_TTL, PropertyType.NUMBER);
        defaults.put(TRANSFORM_CUSTOM_TTL, "0");
        types.put(TRANSFORM_CUSTOM_WRITETIME_INCREMENT, PropertyType.NUMBER);
        defaults.put(TRANSFORM_CUSTOM_WRITETIME_INCREMENT, "0");
        types.put(TRANSFORM_CODECS, PropertyType.STRING_LIST);
        types.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, PropertyType.STRING);
        defaults.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "yyyyMMddHHmmss");
        types.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, PropertyType.STRING);
        defaults.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "UTC");
        types.put(TRANSFORM_MAP_REMOVE_KEY_WITH_NO_VALUE, PropertyType.BOOLEAN);
        defaults.put(TRANSFORM_MAP_REMOVE_KEY_WITH_NO_VALUE, "false");
    }

    // ==========================================================================
    // Cassandra-side Filters
    // ==========================================================================
    public static final String PARTITION_MIN = "spark.cdm.filter.cassandra.partition.min";
    public static final String PARTITION_MAX = "spark.cdm.filter.cassandra.partition.max";
    public static final String FILTER_CQL_WHERE_CONDITION = "spark.cdm.filter.cassandra.whereCondition";
    static {
        types.put(PARTITION_MIN, PropertyType.STRING);
        types.put(PARTITION_MAX, PropertyType.STRING);
        types.put(FILTER_CQL_WHERE_CONDITION, PropertyType.STRING);
    }

    // ==========================================================================
    // Java-side Filters
    // ==========================================================================
    public static final String TOKEN_COVERAGE_PERCENT = "spark.cdm.filter.java.token.percent";
    public static final String FILTER_WRITETS_MIN = "spark.cdm.filter.java.writetime.min";
    public static final String FILTER_WRITETS_MAX = "spark.cdm.filter.java.writetime.max";
    public static final String FILTER_COLUMN_NAME = "spark.cdm.filter.java.column.name";
    public static final String FILTER_COLUMN_VALUE = "spark.cdm.filter.java.column.value";
    static {
        types.put(TOKEN_COVERAGE_PERCENT, PropertyType.NUMBER);
        defaults.put(TOKEN_COVERAGE_PERCENT, "100");
        types.put(FILTER_WRITETS_MIN, PropertyType.NUMBER);
        types.put(FILTER_WRITETS_MAX, PropertyType.NUMBER);
        types.put(FILTER_COLUMN_NAME, PropertyType.STRING);
        types.put(FILTER_COLUMN_VALUE, PropertyType.STRING);
    }

    // ==========================================================================
    // Constant Column Feature
    // ==========================================================================
    public static final String CONSTANT_COLUMN_NAMES = "spark.cdm.feature.constantColumns.names"; // const1,const2
    public static final String CONSTANT_COLUMN_VALUES = "spark.cdm.feature.constantColumns.values"; // 'abcd',1234
    // Regex needed when values have commas
    public static final String CONSTANT_COLUMN_SPLIT_REGEX = "spark.cdm.feature.constantColumns.splitRegex";
    static {
        types.put(CONSTANT_COLUMN_NAMES, PropertyType.STRING_LIST);
        types.put(CONSTANT_COLUMN_VALUES, PropertyType.STRING);
        types.put(CONSTANT_COLUMN_SPLIT_REGEX, PropertyType.STRING);
        defaults.put(CONSTANT_COLUMN_SPLIT_REGEX, ",");
    }

    // ==========================================================================
    // Explode Map Feature
    // ==========================================================================
    public static final String EXPLODE_MAP_ORIGIN_COLUMN_NAME = "spark.cdm.feature.explodeMap.origin.name"; // map_to_explode
    public static final String EXPLODE_MAP_TARGET_KEY_COLUMN_NAME = "spark.cdm.feature.explodeMap.target.name.key"; // map_key
    public static final String EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME = "spark.cdm.feature.explodeMap.target.name.value"; // map_value

    static {
        types.put(EXPLODE_MAP_ORIGIN_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, PropertyType.STRING);
    }

    // ==========================================================================
    // Extract JsonFeature
    // ==========================================================================
    public static final String EXTRACT_JSON_EXCLUSIVE = "spark.cdm.feature.extractJson.exclusive";
    public static final String EXTRACT_JSON_ORIGIN_COLUMN_NAME = "spark.cdm.feature.extractJson.originColumn";
    public static final String EXTRACT_JSON_TARGET_COLUMN_MAPPING = "spark.cdm.feature.extractJson.propertyMapping";
    public static final String EXTRACT_JSON_TARGET_OVERWRITE = "spark.cdm.feature.extractJson.overwrite";

    static {
        types.put(EXTRACT_JSON_EXCLUSIVE, PropertyType.BOOLEAN);
        defaults.put(EXTRACT_JSON_EXCLUSIVE, "false");
        types.put(EXTRACT_JSON_ORIGIN_COLUMN_NAME, PropertyType.STRING);
        types.put(EXTRACT_JSON_TARGET_COLUMN_MAPPING, PropertyType.STRING);
        types.put(EXTRACT_JSON_TARGET_OVERWRITE, PropertyType.BOOLEAN);
        defaults.put(EXTRACT_JSON_TARGET_OVERWRITE, "false");
    }

    // ==========================================================================
    // Guardrail Feature
    // ==========================================================================
    public static final String GUARDRAIL_COLSIZE_KB = "spark.cdm.feature.guardrail.colSizeInKB";
    static {
        types.put(GUARDRAIL_COLSIZE_KB, PropertyType.NUMBER);
        defaults.put(GUARDRAIL_COLSIZE_KB, "0");
    }

    // ==========================================================================
    // Properties that configure origin TLS
    // ==========================================================================
    public static final String ORIGIN_TLS_ENABLED = "spark.cdm.connect.origin.tls.enabled"; // false
    public static final String ORIGIN_TLS_TRUSTSTORE_PATH = "spark.cdm.connect.origin.tls.trustStore.path";
    public static final String ORIGIN_TLS_TRUSTSTORE_PASSWORD = "spark.cdm.connect.origin.tls.trustStore.password";
    public static final String ORIGIN_TLS_TRUSTSTORE_TYPE = "spark.cdm.connect.origin.tls.trustStore.type"; // JKS
    public static final String ORIGIN_TLS_KEYSTORE_PATH = "spark.cdm.connect.origin.tls.keyStore.path";
    public static final String ORIGIN_TLS_KEYSTORE_PASSWORD = "spark.cdm.connect.origin.tls.keyStore.password";
    public static final String ORIGIN_TLS_ALGORITHMS = "spark.cdm.connect.origin.tls.enabledAlgorithms"; // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
    public static final String ORIGIN_TLS_IS_ASTRA = "spark.cdm.connect.origin.tls.isAstra";
    static {
        types.put(ORIGIN_TLS_ENABLED, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_TLS_ENABLED, "false");
        types.put(ORIGIN_TLS_TRUSTSTORE_PATH, PropertyType.STRING);
        types.put(ORIGIN_TLS_TRUSTSTORE_PASSWORD, PropertyType.STRING);
        types.put(ORIGIN_TLS_TRUSTSTORE_TYPE, PropertyType.STRING);
        defaults.put(ORIGIN_TLS_TRUSTSTORE_TYPE, "JKS");
        types.put(ORIGIN_TLS_KEYSTORE_PATH, PropertyType.STRING);
        types.put(ORIGIN_TLS_KEYSTORE_PASSWORD, PropertyType.STRING);
        types.put(ORIGIN_TLS_ALGORITHMS, PropertyType.STRING); // This is a list but it is handled by Spark
        defaults.put(ORIGIN_TLS_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        types.put(ORIGIN_TLS_IS_ASTRA, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_TLS_IS_ASTRA, "false");
    }

    // ==========================================================================
    // Properties that configure target TLS
    // ==========================================================================
    public static final String TARGET_TLS_ENABLED = "spark.cdm.connect.target.tls.enabled"; // false
    public static final String TARGET_TLS_TRUSTSTORE_PATH = "spark.cdm.connect.target.tls.trustStore.path";
    public static final String TARGET_TLS_TRUSTSTORE_PASSWORD = "spark.cdm.connect.target.tls.trustStore.password";
    public static final String TARGET_TLS_TRUSTSTORE_TYPE = "spark.cdm.connect.target.tls.trustStore.type"; // JKS
    public static final String TARGET_TLS_KEYSTORE_PATH = "spark.cdm.connect.target.tls.keyStore.path";
    public static final String TARGET_TLS_KEYSTORE_PASSWORD = "spark.cdm.connect.target.tls.keyStore.password";
    public static final String TARGET_TLS_ALGORITHMS = "spark.cdm.connect.target.tls.enabledAlgorithms"; // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
    public static final String TARGET_TLS_IS_ASTRA = "spark.cdm.connect.target.tls.isAstra";
    static {
        types.put(TARGET_TLS_ENABLED, PropertyType.BOOLEAN);
        defaults.put(TARGET_TLS_ENABLED, "false");
        types.put(TARGET_TLS_TRUSTSTORE_PATH, PropertyType.STRING);
        types.put(TARGET_TLS_TRUSTSTORE_PASSWORD, PropertyType.STRING);
        types.put(TARGET_TLS_TRUSTSTORE_TYPE, PropertyType.STRING);
        defaults.put(TARGET_TLS_TRUSTSTORE_TYPE, "JKS");
        types.put(TARGET_TLS_KEYSTORE_PATH, PropertyType.STRING);
        types.put(TARGET_TLS_KEYSTORE_PASSWORD, PropertyType.STRING);
        types.put(TARGET_TLS_ALGORITHMS, PropertyType.STRING); // This is a list but it is handled by Spark
        defaults.put(TARGET_TLS_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        types.put(TARGET_TLS_IS_ASTRA, PropertyType.BOOLEAN);
        defaults.put(TARGET_TLS_IS_ASTRA, "false");
    }

    // ==========================================================================
    // Properties used for Unit Testing
    // ==========================================================================
    public static final String TEST_STRING = "test.string";
    protected static final String TEST_STRING_DEFAULT = "text";
    public static final String TEST_STRING_NO_DEFAULT = "test.string.noDefault";
    public static final String TEST_STRING_LIST = "test.stringList";
    protected static final String TEST_STRING_LIST_DEFAULT = "text1,text2";
    public static final String TEST_NUMBER = "test.number";
    protected static final String TEST_NUMBER_DEFAULT = "1";
    public static final String TEST_NUMBER_LIST = "test.numberList";
    protected static final String TEST_NUMBER_LIST_DEFAULT = "1,2";
    public static final String TEST_BOOLEAN = "test.boolean";
    protected static final String TEST_BOOLEAN_DEFAULT = "true";
    public static final String TEST_UNHANDLED_TYPE = "test.unhandled.type";
    static {
        types.put(TEST_STRING, PropertyType.STRING);
        defaults.put(TEST_STRING, TEST_STRING_DEFAULT);
        types.put(TEST_STRING_NO_DEFAULT, PropertyType.STRING);
        types.put(TEST_STRING_LIST, PropertyType.STRING_LIST);
        defaults.put(TEST_STRING_LIST, TEST_STRING_LIST_DEFAULT);
        types.put(TEST_NUMBER, PropertyType.NUMBER);
        defaults.put(TEST_NUMBER, TEST_NUMBER_DEFAULT);
        types.put(TEST_NUMBER_LIST, PropertyType.NUMBER_LIST);
        defaults.put(TEST_NUMBER_LIST, TEST_NUMBER_LIST_DEFAULT);
        types.put(TEST_BOOLEAN, PropertyType.BOOLEAN);
        defaults.put(TEST_BOOLEAN, TEST_BOOLEAN_DEFAULT);
        types.put(TEST_UNHANDLED_TYPE, PropertyType.TEST_UNHANDLED_TYPE);
    }

    public static Boolean isKnown(String key) {
        return types.containsKey(key);
    }

    public static Object asType(PropertyType propertyType, String propertyValue) {
        switch (propertyType) {
        case STRING:
            return propertyValue;
        case STRING_LIST:
            return Arrays.asList(propertyValue.split(","));
        case NUMBER:
            try {
                return Long.parseLong(propertyValue);
            } catch (NumberFormatException e) {
                return null;
            }
        case NUMBER_LIST:
            String[] numValues = propertyValue.split(",");
            ArrayList<Number> numbers = new ArrayList<>(numValues.length);
            try {
                for (String value : numValues) {
                    numbers.add(Long.parseLong(value));
                }
                return numbers;
            } catch (NumberFormatException e) {
                return null;
            }
        case BOOLEAN:
            return Boolean.parseBoolean(propertyValue);
        default:
            throw new IllegalArgumentException("Unhandled property type: " + propertyType);
        }
    }

    public static Object getDefault(String key) {
        PropertyType type = types.get(key);
        String value = defaults.get(key);
        if (type == null || value == null) {
            return null;
        }
        return asType(type, value);
    }

    public static String getDefaultAsString(String key) {
        return defaults.get(key);
    }

    public static PropertyType getType(String key) {
        return types.get(key);
    }

    public static Map<String, PropertyType> getTypeMap() {
        return types;
    }

    public static Set<String> getRequired() {
        return required;
    }

    public static boolean validateType(PropertyType expectedType, Object value) {
        switch (expectedType) {
        case STRING:
            if (value instanceof String) {
                return true;
            }
            break;
        case STRING_LIST:
            if (value instanceof List<?>) {
                List<?> list = (List<?>) value;
                if (list.isEmpty()) {
                    return false;
                } else {
                    for (Object o : list) {
                        if (!(o instanceof String)) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            break;
        case NUMBER:
            if (value instanceof Number) {
                return true;
            }
            break;
        case NUMBER_LIST:
            if (value instanceof List<?>) {
                List<?> list = (List<?>) value;
                if (list.isEmpty()) {
                    return false;
                } else {
                    for (Object o : list) {
                        if (!(o instanceof Number)) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            break;
        case BOOLEAN:
            if (value instanceof Boolean) {
                return true;
            }
            break;
        default:
            break;
        }
        return false;
    }
}
