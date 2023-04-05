package datastax.astra.migrate.properties;

import datastax.astra.migrate.MigrateDataType;

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
    // Properties to establish connections to the origin and target databases
    //==========================================================================
    public static final String ORIGIN_CONNECT_HOST           = "spark.origin.host";          // localhost
    public static final String ORIGIN_CONNECT_PORT           = "spark.origin.port";          // 9042
    public static final String ORIGIN_CONNECT_SCB            = "spark.origin.scb";           // file:///aaa/bbb/secure-connect-enterprise.zip
    public static final String ORIGIN_CONNECT_USERNAME       = "spark.origin.username";      // some-username
    public static final String ORIGIN_CONNECT_PASSWORD       = "spark.origin.password";      // some-secret-password
    public static final String TARGET_CONNECT_HOST           = "spark.target.host";          // localhost
    public static final String TARGET_CONNECT_PORT           = "spark.target.port";          // 9042
    public static final String TARGET_CONNECT_SCB            = "spark.target.scb";           // file:///aaa/bbb/secure-connect-enterprise.zip
    public static final String TARGET_CONNECT_USERNAME       = "spark.target.username";      // some-username
    public static final String TARGET_CONNECT_PASSWORD       = "spark.target.password";      // some-secret-password

    public static final String READ_CL                       = "spark.consistency.read";     // LOCAL_QUORUM
    public static final String WRITE_CL                      = "spark.consistency.write";    // LOCAL_QUORUM

    static {
           types.put(ORIGIN_CONNECT_HOST, PropertyType.STRING);
           types.put(ORIGIN_CONNECT_PORT, PropertyType.NUMBER);
        defaults.put(ORIGIN_CONNECT_PORT, "9042");
           types.put(ORIGIN_CONNECT_SCB, PropertyType.STRING);
           types.put(ORIGIN_CONNECT_USERNAME, PropertyType.STRING);
           types.put(ORIGIN_CONNECT_PASSWORD, PropertyType.STRING);

           types.put(TARGET_CONNECT_HOST, PropertyType.STRING);
           types.put(TARGET_CONNECT_PORT, PropertyType.NUMBER);
        defaults.put(TARGET_CONNECT_PORT, "9042");
           types.put(TARGET_CONNECT_SCB, PropertyType.STRING);
           types.put(TARGET_CONNECT_USERNAME, PropertyType.STRING);
           types.put(TARGET_CONNECT_PASSWORD, PropertyType.STRING);

           types.put(READ_CL, PropertyType.STRING);
        defaults.put(READ_CL, "LOCAL_QUORUM");
           types.put(WRITE_CL, PropertyType.STRING);
        defaults.put(WRITE_CL, "LOCAL_QUORUM");
    }

    //==========================================================================
    // Properties that describe the origin table
    //==========================================================================
    public static final String ORIGIN_KEYSPACE_TABLE  = "spark.origin.keyspaceTable";      // test.a1
    public static final String ORIGIN_COLUMN_NAMES    = "spark.query.origin";              // comma-separated-partition-key,comma-separated-clustering-key,comma-separated-other-columns
    public static final String ORIGIN_PARTITION_KEY   = "spark.query.origin.partitionKey"; // comma-separated-partition-key
    public static final String ORIGIN_COLUMN_TYPES    = "spark.query.types";               // 9,1,4,3
    public static final String ORIGIN_TTL_COLS        = "spark.query.ttl.cols";            // 2,3
    public static final String ORIGIN_WRITETIME_COLS  = "spark.query.writetime.cols";      // 2,3
    public static final String ORIGIN_IS_COUNTER      = "spark.counterTable";              // false
    public static final String ORIGIN_COUNTER_CQL     = "spark.counterTable.cql";
    public static final String ORIGIN_COUNTER_INDEXES = "spark.counterTable.cql.index";    // 0
    public static final String ORIGIN_COUNTER_FORCE_WHEN_MISSING = "spark.counterTable.missing.force";    // false
    public static final String ORIGIN_PRIMARY_KEY        = "spark.query.origin.id";         // Defaults to TARGET_PRIMARY_KEY, same config rules
    public static final String ORIGIN_PRIMARY_KEY_TYPES  = "spark.cdm.cql.origin.id.types"; // Code-managed, not an external property

    static {
           types.put(ORIGIN_KEYSPACE_TABLE, PropertyType.STRING);
        required.add(ORIGIN_KEYSPACE_TABLE);
           types.put(ORIGIN_COLUMN_NAMES, PropertyType.STRING_LIST);
        required.add(ORIGIN_COLUMN_NAMES);
           types.put(ORIGIN_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        required.add(ORIGIN_COLUMN_TYPES);
           types.put(ORIGIN_PARTITION_KEY, PropertyType.STRING_LIST);
        required.add(ORIGIN_PARTITION_KEY);
           types.put(ORIGIN_TTL_COLS, PropertyType.NUMBER_LIST);
           types.put(ORIGIN_WRITETIME_COLS, PropertyType.NUMBER_LIST);
           types.put(ORIGIN_IS_COUNTER, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_IS_COUNTER, "false");
           types.put(ORIGIN_COUNTER_CQL, PropertyType.STRING);
           types.put(ORIGIN_COUNTER_INDEXES, PropertyType.NUMBER_LIST);
           types.put(ORIGIN_COUNTER_FORCE_WHEN_MISSING, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_COUNTER_FORCE_WHEN_MISSING, "false");
           types.put(ORIGIN_PRIMARY_KEY, PropertyType.STRING_LIST);
        required.add(ORIGIN_PRIMARY_KEY);
           types.put(ORIGIN_PRIMARY_KEY_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        required.add(ORIGIN_PRIMARY_KEY_TYPES);
    }

    //==========================================================================
    // Properties that filter records from origin
    //==========================================================================
    public static final String ORIGIN_FILTER_CONDITION       = "spark.query.condition";
    public static final String ORIGIN_FILTER_WRITETS_ENABLED = "spark.origin.writeTimeStampFilter";     // false
    public static final String ORIGIN_FILTER_WRITETS_MIN     = "spark.origin.minWriteTimeStampFilter";  // 0
    public static final String ORIGIN_FILTER_WRITETS_MAX     = "spark.origin.maxWriteTimeStampFilter";  // 4102444800000000
    public static final String ORIGIN_FILTER_COLUMN_ENABLED  = "spark.origin.FilterData";               // false
    public static final String ORIGIN_FILTER_COLUMN_NAME     = "spark.origin.FilterColumn";             // test
    public static final String ORIGIN_FILTER_COLUMN_INDEX    = "spark.origin.FilterColumnIndex";        // 2
    public static final String ORIGIN_FILTER_COLUMN_TYPE     = "spark.origin.FilterColumnType";         // 6%16
    public static final String ORIGIN_FILTER_COLUMN_VALUE    = "spark.origin.FilterColumnValue";        // test
    public static final String ORIGIN_COVERAGE_PERCENT       = "spark.coveragePercent";                 // 100
    public static final String ORIGIN_HAS_RANDOM_PARTITIONER = "spark.origin.hasRandomPartitioner";     // false

    public static final String ORIGIN_CHECK_COLSIZE_ENABLED      = "spark.origin.checkTableforColSize";            // false
    public static final String ORIGIN_CHECK_COLSIZE_COLUMN_NAMES = "spark.origin.checkTableforColSize.cols";       // partition-key,clustering-key
    public static final String ORIGIN_CHECK_COLSIZE_COLUMN_TYPES = "spark.origin.checkTableforColSize.cols.types"; // 9,1

    static {
           types.put(ORIGIN_FILTER_CONDITION, PropertyType.STRING);
           types.put(ORIGIN_FILTER_WRITETS_ENABLED, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_FILTER_WRITETS_ENABLED, "false");
           types.put(ORIGIN_FILTER_WRITETS_MIN, PropertyType.NUMBER);
        defaults.put(ORIGIN_FILTER_WRITETS_MIN, "0");
           types.put(ORIGIN_FILTER_WRITETS_MAX, PropertyType.NUMBER);
        defaults.put(ORIGIN_FILTER_WRITETS_MAX, "0");
           types.put(ORIGIN_FILTER_COLUMN_ENABLED, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_FILTER_COLUMN_ENABLED, "false");
           types.put(ORIGIN_FILTER_COLUMN_NAME, PropertyType.STRING);
           types.put(ORIGIN_FILTER_COLUMN_INDEX, PropertyType.NUMBER);
        defaults.put(ORIGIN_FILTER_COLUMN_INDEX, "0");
           types.put(ORIGIN_FILTER_COLUMN_TYPE, PropertyType.MIGRATION_TYPE);
           types.put(ORIGIN_FILTER_COLUMN_VALUE, PropertyType.STRING);
           types.put(ORIGIN_COVERAGE_PERCENT, PropertyType.NUMBER);
        defaults.put(ORIGIN_COVERAGE_PERCENT, "100");
           types.put(ORIGIN_HAS_RANDOM_PARTITIONER, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_HAS_RANDOM_PARTITIONER, "false");

           types.put(ORIGIN_CHECK_COLSIZE_ENABLED, PropertyType.BOOLEAN);
        defaults.put(ORIGIN_CHECK_COLSIZE_ENABLED, "false");
           types.put(ORIGIN_CHECK_COLSIZE_COLUMN_NAMES, PropertyType.STRING_LIST);
           types.put(ORIGIN_CHECK_COLSIZE_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
    }

    //==========================================================================
    // Properties that describe the target table
    //==========================================================================
    public static final String TARGET_KEYSPACE_TABLE       = "spark.target.keyspaceTable";        // test.a1
    public static final String TARGET_PRIMARY_KEY          = "spark.query.target.id";             // comma-separated-partition-key,comma-separated-clustering-key
    public static final String TARGET_PRIMARY_KEY_TYPES    = "spark.cdm.cql.target.id.types";     // Code-managed, not an external property
    public static final String TARGET_COLUMN_NAMES         = "spark.query.target";
    public static final String TARGET_COLUMN_TYPES         = "spark.cdm.cql.target.types";        // Code-managed, not an external property
    public static final String TARGET_CUSTOM_WRITETIME     = "spark.target.custom.writeTime";     // 0
    public static final String TARGET_AUTOCORRECT_MISSING  = "spark.target.autocorrect.missing";  // false
    public static final String TARGET_AUTOCORRECT_MISMATCH = "spark.target.autocorrect.mismatch"; // false
    public static final String TARGET_REPLACE_MISSING_TS   = "spark.target.replace.blankTimestampKeyUsingEpoch";
    static {
           types.put(TARGET_KEYSPACE_TABLE, PropertyType.STRING);
        required.add(TARGET_KEYSPACE_TABLE);
           types.put(TARGET_PRIMARY_KEY, PropertyType.STRING_LIST);
        required.add(TARGET_PRIMARY_KEY);
           types.put(TARGET_PRIMARY_KEY_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        required.add(TARGET_PRIMARY_KEY_TYPES);
           types.put(TARGET_COLUMN_NAMES, PropertyType.STRING_LIST);
        required.add(TARGET_COLUMN_NAMES); // we need this, though it should be defaulted with ORIGIN_COLUMN_NAMES value
           types.put(TARGET_COLUMN_TYPES, PropertyType.MIGRATION_TYPE_LIST);
        required.add(TARGET_COLUMN_TYPES);
           types.put(TARGET_CUSTOM_WRITETIME, PropertyType.NUMBER);
        defaults.put(TARGET_CUSTOM_WRITETIME, "0");
           types.put(TARGET_AUTOCORRECT_MISSING, PropertyType.BOOLEAN);
        defaults.put(TARGET_AUTOCORRECT_MISSING, "false");
           types.put(TARGET_AUTOCORRECT_MISMATCH, PropertyType.BOOLEAN);
        defaults.put(TARGET_AUTOCORRECT_MISMATCH, "false");
           types.put(TARGET_REPLACE_MISSING_TS, PropertyType.NUMBER);
    }

    //==========================================================================
    // Properties that adjust performance and error handling
    //==========================================================================
    public static final String SPARK_LIMIT_READ  = "spark.readRateLimit";         // 20000
    public static final String SPARK_LIMIT_WRITE = "spark.writeRateLimit";        // 40000
    public static final String SPARK_NUM_SPLITS  = "spark.numSplits";             // 10000, was spark.splitSize
    public static final String DEPRECATED_SPARK_SPLIT_SIZE  = "spark.splitSize";  // 10000
    public static final String SPARK_BATCH_SIZE  = "spark.batchSize";             // 5
    public static final String SPARK_MAX_RETRIES = "spark.maxRetries";            // 0
    public static final String READ_FETCH_SIZE   = "spark.read.fetch.sizeInRows"; //1000
    public static final String SPARK_STATS_AFTER = "spark.printStatsAfter";       //100000
    public static final String FIELD_GUARDRAIL_MB = "spark.fieldGuardraillimitMB"; //10
    public static final String PARTITION_MIN     = "spark.origin.minPartition";   // -9223372036854775808
    public static final String PARTITION_MAX     = "spark.origin.maxPartition";   // 9223372036854775807

    static {
           types.put(SPARK_LIMIT_READ, PropertyType.NUMBER);
        defaults.put(SPARK_LIMIT_READ, "20000");
           types.put(SPARK_LIMIT_WRITE, PropertyType.NUMBER);
        defaults.put(SPARK_LIMIT_WRITE, "40000");
           types.put(SPARK_NUM_SPLITS, PropertyType.NUMBER);
        defaults.put(SPARK_NUM_SPLITS, "10000");
           types.put(DEPRECATED_SPARK_SPLIT_SIZE, PropertyType.NUMBER);
        defaults.put(DEPRECATED_SPARK_SPLIT_SIZE, "10000");
           types.put(SPARK_BATCH_SIZE, PropertyType.NUMBER);
        defaults.put(SPARK_BATCH_SIZE, "5");
           types.put(SPARK_MAX_RETRIES, PropertyType.NUMBER);
        defaults.put(SPARK_MAX_RETRIES, "0");
           types.put(READ_FETCH_SIZE, PropertyType.NUMBER);
        defaults.put(READ_FETCH_SIZE, "1000");
           types.put(SPARK_STATS_AFTER, PropertyType.NUMBER);
        defaults.put(SPARK_STATS_AFTER, "100000");
           types.put(FIELD_GUARDRAIL_MB, PropertyType.NUMBER);
        defaults.put(FIELD_GUARDRAIL_MB, "0");
           types.put(PARTITION_MIN, PropertyType.NUMBER);
        defaults.put(PARTITION_MIN, "-9223372036854775808");
           types.put(PARTITION_MAX, PropertyType.NUMBER);
        defaults.put(PARTITION_MAX, "9223372036854775807");

    }

    //==========================================================================
    // Properties that configure origin TLS
    //==========================================================================
    public static final String ORIGIN_TLS_ENABLED             = "spark.origin.ssl.enabled";         // false
    public static final String ORIGIN_TLS_TRUSTSTORE_PATH     = "spark.origin.trustStore.path";
    public static final String ORIGIN_TLS_TRUSTSTORE_PASSWORD = "spark.origin.trustStore.password";
    public static final String ORIGIN_TLS_TRUSTSTORE_TYPE     = "spark.origin.trustStore.type";     // JKS
    public static final String ORIGIN_TLS_KEYSTORE_PATH       = "spark.origin.keyStore.path";
    public static final String ORIGIN_TLS_KEYSTORE_PASSWORD   = "spark.origin.keyStore.password";
    public static final String ORIGIN_TLS_ALGORITHMS          = "spark.origin.enabledAlgorithms";   // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
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
    public static final String TARGET_TLS_ENABLED             = "spark.target.ssl.enabled";         // false
    public static final String TARGET_TLS_TRUSTSTORE_PATH     = "spark.target.trustStore.path";
    public static final String TARGET_TLS_TRUSTSTORE_PASSWORD = "spark.target.trustStore.password";
    public static final String TARGET_TLS_TRUSTSTORE_TYPE     = "spark.target.trustStore.type";     // JKS
    public static final String TARGET_TLS_KEYSTORE_PATH       = "spark.target.keyStore.path";
    public static final String TARGET_TLS_KEYSTORE_PASSWORD   = "spark.target.keyStore.password";
    public static final String TARGET_TLS_ALGORITHMS          = "spark.target.enabledAlgorithms";   // TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
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
    // Constant Column Feature
    //==========================================================================
    public static final String CONSTANT_COLUMN_NAMES                 = "spark.cdm.cql.feature.constantColumns.names";        // const1,const2
    public static final String CONSTANT_COLUMN_TYPES                 = "spark.cdm.cql.feature.constantColumns.types";        // 0,1
    public static final String CONSTANT_COLUMN_VALUES                = "spark.cdm.cql.feature.constantColumns.values";       // 'abcd',1234
    public static final String CONSTANT_COLUMN_SPLIT_REGEX           = "spark.cdm.cql.feature.constantColumns.splitRegex";   // , : this is needed because hard-coded values can have commas in them
    public static final String TARGET_PRIMARY_KEY_TYPES_NO_CONSTANTS = "spark.cdm.cql.feature.constantColumns.noConstants";  // 1 : this will be set within the code

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
    public static final String EXPLODE_MAP_ORIGIN_COLUMN_NAME        = "spark.cdm.cql.feature.explodeMap.origin.name";         // map_to_explode
    public static final String EXPLODE_MAP_TARGET_KEY_COLUMN_NAME    = "spark.cdm.cql.feature.explodeMap.target.name.key";     // map_key
    public static final String EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME  = "spark.cdm.cql.feature.explodeMap.target.name.value";   // map_value

    static {
        types.put(EXPLODE_MAP_ORIGIN_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, PropertyType.STRING);
        types.put(EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, PropertyType.STRING);
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
