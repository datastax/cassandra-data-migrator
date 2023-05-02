package com.datastax.cdm.properties;

import com.datastax.cdm.job.MigrateDataType;

import java.util.*;

public class KnownProperties {

    public enum PropertyType {
        STRING,
        NUMBER,
        BOOLEAN,
        STRING_LIST,
        NUMBER_LIST,
        MIGRATION_TYPE,
        MIGRATION_TYPE_LIST,
        TEST_UNHANDLED_TYPE
    }

    private static Map<String,PropertyType> types = new HashMap<>();
    private static Map<String,String> defaults = new HashMap<>();
    private static Set<String> required = new HashSet<>();

    //==========================================================================
    // Common connection parameters
    //==========================================================================
    public static final String ORIGIN_CONNECT_HOST           = "spark.cdm.origin.connect.host";
    public static final String ORIGIN_CONNECT_PORT           = "spark.cdm.origin.connect.port";
    public static final String ORIGIN_CONNECT_SCB            = "spark.cdm.origin.connect.scb";
    public static final String ORIGIN_CONNECT_USERNAME       = "spark.cdm.origin.connect.username";
    public static final String ORIGIN_CONNECT_PASSWORD       = "spark.cdm.origin.connect.password";


    public static final String TARGET_CONNECT_HOST           = "spark.cdm.target.connect.host";
    public static final String TARGET_CONNECT_PORT           = "spark.cdm.target.connect.port";
    public static final String TARGET_CONNECT_SCB            = "spark.cdm.target.connect.scb";
    public static final String TARGET_CONNECT_USERNAME       = "spark.cdm.target.connect.username";
    public static final String TARGET_CONNECT_PASSWORD       = "spark.cdm.target.connect.password";

    static {
           types.put(ORIGIN_CONNECT_HOST, PropertyType.STRING);
        defaults.put(ORIGIN_CONNECT_HOST, "localhost");
           types.put(ORIGIN_CONNECT_PORT, PropertyType.NUMBER);
        defaults.put(ORIGIN_CONNECT_PORT, "9042");
           types.put(ORIGIN_CONNECT_SCB, PropertyType.STRING);
           types.put(ORIGIN_CONNECT_USERNAME, PropertyType.STRING);
        defaults.put(ORIGIN_CONNECT_USERNAME, "cassandra");
           types.put(ORIGIN_CONNECT_PASSWORD, PropertyType.STRING);
        defaults.put(ORIGIN_CONNECT_PASSWORD, "cassandra");

           types.put(TARGET_CONNECT_HOST, PropertyType.STRING);
        defaults.put(TARGET_CONNECT_HOST, "localhost");
           types.put(TARGET_CONNECT_PORT, PropertyType.NUMBER);
        defaults.put(TARGET_CONNECT_PORT, "9042");
           types.put(TARGET_CONNECT_SCB, PropertyType.STRING);
           types.put(TARGET_CONNECT_USERNAME, PropertyType.STRING);
        defaults.put(TARGET_CONNECT_USERNAME, "cassandra");
           types.put(TARGET_CONNECT_PASSWORD, PropertyType.STRING);
        defaults.put(TARGET_CONNECT_PASSWORD, "cassandra");

    }

    //==========================================================================
    // Properties that describe the origin schema
    //==========================================================================
    public static final String ORIGIN_KEYSPACE_TABLE         = "spark.cdm.schema.origin.keyspaceTable";
    public static final String ORIGIN_COLUMN_NAMES           = "spark.cdm.schema.origin.column.names";
    public static final String ORIGIN_PARTITION_KEY          = "spark.cdm.schema.origin.column.partition.names";
    public static final String ORIGIN_COLUMN_TYPES           = "spark.cdm.schema.origin.column.types";
    public static final String ORIGIN_TTL_INDEXES            = "spark.cdm.schema.origin.column.ttl.indexes";
    public static final String ORIGIN_WRITETIME_INDEXES      = "spark.cdm.schema.origin.column.writetime.indexes";

    public static final String ORIGIN_HAS_RANDOM_PARTITIONER = "spark.cdm.schema.origin.randomPartitioner";
    public static final String ORIGIN_COUNTER_INDEXES        = "spark.cdm.schema.origin.column.counter.indexes";
    public static final String ORIGIN_PRIMARY_KEY_TYPES      = "spark.cdm.schema.origin.column.id.types";
    public static final String ORIGIN_COLUMN_NAMES_TO_TARGET = "spark.cdm.schema.origin.column.names.to.target";

    // Not exposed in .properties
    public static final String ORIGIN_PRIMARY_KEY_NAMES      = "spark.cdm.schema.origin.column.id.names";


    static {
           types.put(ORIGIN_KEYSPACE_TABLE, PropertyType.STRING);
        required.add(ORIGIN_KEYSPACE_TABLE);
           types.put(ORIGIN_COLUMN_NAMES, PropertyType.STRING_LIST);
        required.add(ORIGIN_COLUMN_NAMES);
           types.put(ORIGIN_PARTITION_KEY, PropertyType.STRING_LIST);
        required.add(ORIGIN_PARTITION_KEY);
           types.put(ORIGIN_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        required.add(ORIGIN_COLUMN_TYPES);
           types.put(ORIGIN_TTL_INDEXES, PropertyType.NUMBER_LIST);
           types.put(ORIGIN_WRITETIME_INDEXES, PropertyType.NUMBER_LIST);

           types.put(ORIGIN_HAS_RANDOM_PARTITIONER, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_HAS_RANDOM_PARTITIONER, "false");
           types.put(ORIGIN_COUNTER_INDEXES, PropertyType.NUMBER_LIST);
           types.put(ORIGIN_PRIMARY_KEY_TYPES, PropertyType.MIGRATION_TYPE_LIST);
           types.put(ORIGIN_COLUMN_NAMES_TO_TARGET, PropertyType.STRING_LIST);

           types.put(ORIGIN_PRIMARY_KEY_NAMES, PropertyType.STRING_LIST);
    }

    //==========================================================================
    // Properties that describe the target schema
    //==========================================================================
    public static final String TARGET_KEYSPACE_TABLE         = "spark.cdm.schema.target.keyspaceTable";
    public static final String TARGET_PRIMARY_KEY            = "spark.cdm.schema.target.column.id.names";
    public static final String TARGET_COLUMN_NAMES           = "spark.cdm.schema.target.column.names";

    // Not exposed in .properties
    public static final String TARGET_COLUMN_TYPES         = "spark.cdm.schema.target.column.types";
    public static final String TARGET_PRIMARY_KEY_TYPES    = "spark.cdm.schema.target.column.id.types";

    static {
           types.put(TARGET_KEYSPACE_TABLE, PropertyType.STRING);
        required.add(TARGET_KEYSPACE_TABLE);
           types.put(TARGET_PRIMARY_KEY, PropertyType.STRING_LIST);
        required.add(TARGET_PRIMARY_KEY);
           types.put(TARGET_COLUMN_NAMES, PropertyType.STRING_LIST);

           types.put(TARGET_PRIMARY_KEY_TYPES, PropertyType.MIGRATION_TYPE_LIST);
           types.put(TARGET_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
    }

    //==========================================================================
    // Autocorrection, Performance, and Operations Parameters
    //==========================================================================
    public static final String AUTOCORRECT_MISSING              = "spark.cdm.autocorrect.missing";    // false
    public static final String AUTOCORRECT_MISMATCH             = "spark.cdm.autocorrect.mismatch";   // false
    public static final String AUTOCORRECT_MISSING_COUNTER      = "spark.cdm.autocorrect.missing.counter";  // false

    public static final String PERF_NUM_PARTS                   = "spark.cdm.perfops.numParts";             // 10000, was spark.splitSize
    public static final String PERF_BATCH_SIZE                  = "spark.cdm.perfops.batchSize";             // 5
    public static final String PERF_LIMIT_READ                  = "spark.cdm.perfops.readRateLimit";         // 20000
    public static final String PERF_LIMIT_WRITE                 = "spark.cdm.perfops.writeRateLimit";        // 40000

    public static final String READ_CL                          = "spark.cdm.perfops.consistency.read";
    public static final String WRITE_CL                         = "spark.cdm.perfops.consistency.write";
    public static final String PERF_FETCH_SIZE                  = "spark.cdm.perfops.fetchSizeInRows";
    public static final String MAX_RETRIES                      = "spark.cdm.perfops.error.limit";
    public static final String PRINT_STATS_AFTER                = "spark.cdm.perfops.printStatsAfter";

    static {
           types.put(AUTOCORRECT_MISSING, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISSING, "false");
           types.put(AUTOCORRECT_MISMATCH, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISMATCH, "false");
           types.put(AUTOCORRECT_MISSING_COUNTER, PropertyType.BOOLEAN);
        defaults.put(AUTOCORRECT_MISSING_COUNTER, "false");

        types.put(PERF_NUM_PARTS, PropertyType.NUMBER);
        defaults.put(PERF_NUM_PARTS, "10000");
           types.put(PERF_BATCH_SIZE, PropertyType.NUMBER);
        defaults.put(PERF_BATCH_SIZE, "5");
           types.put(PERF_LIMIT_READ, PropertyType.NUMBER);
        defaults.put(PERF_LIMIT_READ, "20000");
           types.put(PERF_LIMIT_WRITE, PropertyType.NUMBER);
        defaults.put(PERF_LIMIT_WRITE, "40000");

           types.put(READ_CL, PropertyType.STRING);
        defaults.put(READ_CL, "LOCAL_QUORUM");
           types.put(WRITE_CL, PropertyType.STRING);
        defaults.put(WRITE_CL, "LOCAL_QUORUM");
           types.put(PRINT_STATS_AFTER, PropertyType.NUMBER);
        defaults.put(PRINT_STATS_AFTER, "100000");
           types.put(PERF_FETCH_SIZE, PropertyType.NUMBER);
        defaults.put(PERF_FETCH_SIZE, "1000");
           types.put(MAX_RETRIES, PropertyType.NUMBER);
        defaults.put(MAX_RETRIES, "0");
    }

    //==========================================================================
    // Guardrails and Transformations
    //==========================================================================
    public static final String TRANSFORM_REPLACE_MISSING_TS                    = "spark.cdm.transform.missing.key.ts.replace.value";
    public static final String TRANSFORM_CUSTOM_WRITETIME                      = "spark.cdm.transform.custom.writetime";
    public static final String TRANSFORM_CODECS                                = "spark.cdm.transform.codecs";
    public static final String TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT        = "spark.cdm.transform.codecs.timestamp.string.format";
    public static final String TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE   = "spark.cdm.transform.codecs.timestamp.string.zone";



//    TODO: 3.3.0 refactored how guardrails are handled, this needs to be merged forward
//    public static final String GUARDRAIL_FIELD_LIMIT_MB         = "spark.guardrail.colSizeInKB"; //10

    static {
        types.put(TRANSFORM_REPLACE_MISSING_TS, PropertyType.NUMBER);
        types.put(TRANSFORM_CUSTOM_WRITETIME, PropertyType.NUMBER);
     defaults.put(TRANSFORM_CUSTOM_WRITETIME, "0");
        types.put(TRANSFORM_CODECS, PropertyType.STRING_LIST);
        types.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, PropertyType.STRING);
     defaults.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT, "yyyyMMddHHmmss");
        types.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, PropertyType.STRING);
     defaults.put(TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE, "UTC");


     //           types.put(GUARDRAIL_FIELD_LIMIT_MB, PropertyType.NUMBER);
//        defaults.put(GUARDRAIL_FIELD_LIMIT_MB, "0");
    }

    //==========================================================================
    // Cassandra-side Filters
    //==========================================================================
    public static final String PARTITION_MIN               = "spark.cdm.filter.cassandra.partition.min";
    public static final String PARTITION_MAX               = "spark.cdm.filter.cassandra.partition.max";
    public static final String FILTER_CQL_WHERE_CONDITION  = "spark.cdm.filter.cassandra.where.condition";
    static {
           types.put(PARTITION_MIN, PropertyType.NUMBER);
        defaults.put(PARTITION_MIN, "-9223372036854775808");
           types.put(PARTITION_MAX, PropertyType.NUMBER);
        defaults.put(PARTITION_MAX, "9223372036854775807");
           types.put(FILTER_CQL_WHERE_CONDITION, PropertyType.STRING);
    }

    //==========================================================================
    // Java-side Filters
    //==========================================================================
    public static final String TOKEN_COVERAGE_PERCENT        = "spark.cdm.filter.java.token.percent";
    public static final String FILTER_WRITETS_MIN            = "spark.cdm.filter.java.writetime.min";
    public static final String FILTER_WRITETS_MAX            = "spark.cdm.filter.java.writetime.max";
    public static final String FILTER_COLUMN_NAME            = "spark.cdm.filter.java.column.name";
    public static final String FILTER_COLUMN_VALUE           = "spark.cdm.filter.java.column.value";
    static {
           types.put(TOKEN_COVERAGE_PERCENT, PropertyType.NUMBER);
        defaults.put(TOKEN_COVERAGE_PERCENT, "100");
           types.put(FILTER_WRITETS_MIN, PropertyType.NUMBER);
           types.put(FILTER_WRITETS_MAX, PropertyType.NUMBER);
           types.put(FILTER_COLUMN_NAME, PropertyType.STRING);
           types.put(FILTER_COLUMN_VALUE, PropertyType.STRING);
    }

    //==========================================================================
    // Constant Column Feature
    //==========================================================================
    public static final String CONSTANT_COLUMN_NAMES                 = "spark.cdm.feature.constantColumns.names";        // const1,const2
    public static final String CONSTANT_COLUMN_TYPES                 = "spark.cdm.feature.constantColumns.types";        // 0,1
    public static final String CONSTANT_COLUMN_VALUES                = "spark.cdm.feature.constantColumns.values";       // 'abcd',1234
    public static final String CONSTANT_COLUMN_SPLIT_REGEX           = "spark.cdm.feature.constantColumns.splitRegex";   // , : this is needed because hard-coded values can have commas in them
    public static final String TARGET_PRIMARY_KEY_TYPES_NO_CONSTANTS = "spark.cdm.feature.constantColumns.noConstants";  // 1 : this will be set within the code

    static {
        types.put(CONSTANT_COLUMN_NAMES, PropertyType.STRING_LIST);
        types.put(CONSTANT_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        types.put(CONSTANT_COLUMN_VALUES, PropertyType.STRING);
        types.put(CONSTANT_COLUMN_SPLIT_REGEX, PropertyType.STRING);
        types.put(TARGET_PRIMARY_KEY_TYPES_NO_CONSTANTS, PropertyType.MIGRATION_TYPE_LIST);
        defaults.put(CONSTANT_COLUMN_SPLIT_REGEX, ",");
    }

    //==========================================================================
    // Explode Map Feature
    //==========================================================================
    public static final String EXPLODE_MAP_ORIGIN_COLUMN_NAME        = "spark.cdm.feature.explodeMap.origin.name";         // map_to_explode
    public static final String EXPLODE_MAP_TARGET_KEY_COLUMN_NAME    = "spark.cdm.feature.explodeMap.target.name.key";     // map_key
    public static final String EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME  = "spark.cdm.feature.explodeMap.target.name.value";   // map_value

    static {
        types.put(EXPLODE_MAP_ORIGIN_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, PropertyType.STRING);
    }

    //==========================================================================
    // ** Deprecated ** ColSize Check Feature (OriginCountJobSession.java)
    //==========================================================================
//    public static final String ORIGIN_CHECK_COLSIZE_ENABLED      = "spark.cdm.feature.colsizeCheck.checkTableforColSize";            // false
//    public static final String ORIGIN_CHECK_COLSIZE_COLUMN_NAMES = "spark.cdm.feature.colsizeCheck.checkTableforColSize.cols";       // partition-key,clustering-key
//    public static final String ORIGIN_CHECK_COLSIZE_COLUMN_TYPES = "spark.cdm.feature.colsizeCheck.checkTableforColSize.cols.types"; // 9,1
//    static {
//           types.put(ORIGIN_CHECK_COLSIZE_ENABLED, PropertyType.BOOLEAN);
//        defaults.put(ORIGIN_CHECK_COLSIZE_ENABLED, "false");
//           types.put(ORIGIN_CHECK_COLSIZE_COLUMN_NAMES, PropertyType.STRING_LIST);
//           types.put(ORIGIN_CHECK_COLSIZE_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
//    }

    //==========================================================================
    // Properties that configure origin TLS
    //==========================================================================
    public static final String ORIGIN_TLS_ENABLED             = "spark.cdm.origin.connect.tls.enabled";         // false
    public static final String ORIGIN_TLS_TRUSTSTORE_PATH     = "spark.cdm.origin.connect.tls.trustStore.path";
    public static final String ORIGIN_TLS_TRUSTSTORE_PASSWORD = "spark.cdm.origin.connect.tls.trustStore.password";
    public static final String ORIGIN_TLS_TRUSTSTORE_TYPE     = "spark.cdm.origin.connect.tls.trustStore.type";     // JKS
    public static final String ORIGIN_TLS_KEYSTORE_PATH       = "spark.cdm.origin.connect.tls.keyStore.path";
    public static final String ORIGIN_TLS_KEYSTORE_PASSWORD   = "spark.cdm.origin.connect.tls.keyStore.password";
    public static final String ORIGIN_TLS_ALGORITHMS          = "spark.cdm.origin.connect.tls.enabledAlgorithms";   // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
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
    }

    //==========================================================================
    // Properties that configure target TLS
    //==========================================================================
    public static final String TARGET_TLS_ENABLED             = "spark.cdm.target.connect.tls.enabled";         // false
    public static final String TARGET_TLS_TRUSTSTORE_PATH     = "spark.cdm.target.connect.tls.trustStore.path";
    public static final String TARGET_TLS_TRUSTSTORE_PASSWORD = "spark.cdm.target.connect.tls.trustStore.password";
    public static final String TARGET_TLS_TRUSTSTORE_TYPE     = "spark.cdm.target.connect.tls.trustStore.type";     // JKS
    public static final String TARGET_TLS_KEYSTORE_PATH       = "spark.cdm.target.connect.tls.keyStore.path";
    public static final String TARGET_TLS_KEYSTORE_PASSWORD   = "spark.cdm.target.connect.tls.keyStore.password";
    public static final String TARGET_TLS_ALGORITHMS          = "spark.cdm.target.connect.tls.enabledAlgorithms";   // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
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
    }

    //==========================================================================
    // Properties used for Unit Testing
    //==========================================================================
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
    public static final String TEST_MIGRATE_TYPE = "test.migrateType";
    protected static final String TEST_MIGRATE_TYPE_DEFAULT = "0";
    public static final String TEST_MIGRATE_TYPE_LIST = "test.migrateTypeList";
    protected static final String TEST_MIGRATE_TYPE_LIST_DEFAULT = "0,1,2";
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
           types.put(TEST_MIGRATE_TYPE, PropertyType.MIGRATION_TYPE);
        defaults.put(TEST_MIGRATE_TYPE, TEST_MIGRATE_TYPE_DEFAULT);
           types.put(TEST_MIGRATE_TYPE_LIST, PropertyType.MIGRATION_TYPE_LIST);
        defaults.put(TEST_MIGRATE_TYPE_LIST, TEST_MIGRATE_TYPE_LIST_DEFAULT);
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
            case MIGRATION_TYPE:
                return new MigrateDataType(propertyValue);
            case MIGRATION_TYPE_LIST:
                String[] typeValues = propertyValue.split(",");
                ArrayList<MigrateDataType> types = new ArrayList<>(typeValues.length);
                for (String value : typeValues) {
                    types.add(new MigrateDataType(value));
                }
                return types;
            default:
                throw new IllegalArgumentException("Unhandled property type: " + propertyType);
        }
    }

    public static Object getDefault(String key) {
        PropertyType type = types.get(key);
        String value = defaults.get(key);
        if (type == null ||
                value == null) {
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

    public static Map<String,PropertyType> getTypeMap() { return types;}

    public static Set<String> getRequired() { return required; }

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
            case MIGRATION_TYPE:
                if ((value instanceof MigrateDataType) && ((MigrateDataType) value).isValid()) {
                    return true;
                }
                break;
            case MIGRATION_TYPE_LIST:
                if (value instanceof List<?>) {
                    List<?> list = (List<?>) value;
                    if (list.isEmpty()) {
                        return false;
                    } else {
                        for (Object o : list) {
                            if (!(o instanceof MigrateDataType) || !((MigrateDataType) o).isValid()) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                break;
            default:
                break;
        }
        return false;
    }
}
