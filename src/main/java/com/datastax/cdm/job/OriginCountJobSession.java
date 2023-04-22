package com.datastax.cdm.job;

/**
 *
 *  NOTE: this class is broken. Per Pravin:
 *
 *  That class is most likely broken & thay prop spark.query.cols.id.types was used to find
 *  the data-types of primary-key columns. The tool (Migration & Validation) was improved to
 *  auto-detect those types from the main types, so its no longer used in the main classes
 *  any more. It was used for iHeartMedia to check on guardrail issues during migration.
 *
 * Mainly Mike had coded that class, i wud like it to be rewritten & incorporated in the main
 * class (with a param for guardrail check), so that we do not have to maintain a completely
 * different utility.
 *
 * Its difficult to maintain it in the current form, but the feature itself is useful in some
 * scenarios
 */

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

@Deprecated
public class OriginCountJobSession extends BaseJobSession {
//    private static OriginCountJobSession originCountJobSession;
//    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
//    protected AtomicLong readCounter = new AtomicLong(0);
//    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();
//    protected Boolean checkTableforColSize;
//    protected String checkTableforselectCols;
//    protected Integer fieldGuardraillimitMB;
//    protected List<MigrateDataType> checkTableforColSizeTypes = new ArrayList<MigrateDataType>();
//
    protected OriginCountJobSession(CqlSession originSession, SparkConf sc) {
        super(sc);
//        this.originSessionSession = originSession;
//        batchSize = propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
//        printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
//        if (!propertyHelper.meetsMinimum(KnownProperties.SPARK_STATS_AFTER, printStatsAfter, 1)) {
//            logger.warn(KnownProperties.SPARK_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
//            propertyHelper.setProperty(KnownProperties.SPARK_STATS_AFTER, KnownProperties.getDefault(KnownProperties.SPARK_STATS_AFTER));
//            printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
//        }
//
//        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_READ));
//        originKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
//
//        hasRandomPartitioner = propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
//        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
//
//        checkTableforColSize = propertyHelper.getBoolean(KnownProperties.ORIGIN_CHECK_COLSIZE_ENABLED);
//        checkTableforselectCols = propertyHelper.getAsString(KnownProperties.ORIGIN_CHECK_COLSIZE_COLUMN_NAMES);
//        checkTableforColSizeTypes = getTypes(propertyHelper.getAsString(KnownProperties.ORIGIN_CHECK_COLSIZE_COLUMN_TYPES));
//        filterColName = propertyHelper.getAsString(KnownProperties.ORIGIN_FILTER_COLUMN_NAME);
//        filterColType = propertyHelper.getAsString(KnownProperties.ORIGIN_FILTER_COLUMN_TYPE);
//        filterColIndex = propertyHelper.getInteger(KnownProperties.ORIGIN_FILTER_COLUMN_INDEX);
//        fieldGuardraillimitMB = propertyHelper.getInteger(KnownProperties.FIELD_GUARDRAIL_MB);
//
//        String partionKey = propertyHelper.getAsString(KnownProperties.ORIGIN_PARTITION_KEY);
//        idColTypes = getTypes(propertyHelper.getAsString(KnownProperties.TARGET_PRIMARY_KEY_TYPES));
//
//        String selectCols = propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_NAMES);
//        String updateSelectMappingStr = propertyHelper.getAsString(KnownProperties.ORIGIN_COUNTER_INDEXES);
//        for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
//            updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
//        }
//        String originSelectCondition = propertyHelper.getAsString(KnownProperties.ORIGIN_FILTER_CONDITION);
//        // TODO: AbstractJobSession has some checks to ensure AND is added to the condition
//        originSelectStatement = originSession.prepare(
//                "select " + selectCols + " from " + originKeyspaceTable + " where token(" + partionKey.trim()
//                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + originSelectCondition + " ALLOW FILTERING");
    }
//    public static OriginCountJobSession getInstance(CqlSession originSession, SparkConf sparkConf) {
//        if (originCountJobSession == null) {
//            synchronized (OriginCountJobSession.class) {
//                if (originCountJobSession == null) {
//                    originCountJobSession = new OriginCountJobSession(originSession, sparkConf);
//                }
//            }
//        }
//
//        return originCountJobSession;
//    }
//
//    public void getData(BigInteger min, BigInteger max) {
//        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
//        boolean done = false;
//        int maxAttempts = maxRetries + 1;
//        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
//            try {
//                ResultSet resultSet = originSessionSession.execute(originSelectStatement.bind(hasRandomPartitioner ?
//                                min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact())
//                        .setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));
//
//                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();
//
//                // cannot do batching if the writeFilter is greater than 0 or
//                // maxWriteTimeStampFilter is less than max long
//                // do not batch for counters as it adds latency & increases chance of discrepancy
//                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
//                    for (Row originRow : resultSet) {
//                        readLimiter.acquire(1);
//
//                        if (checkTableforColSize) {
//                            int rowColcnt = GetRowColumnLength(originRow, filterColType, filterColIndex);
//                            String result = "";
//                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
//                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
//                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
//                                    Object colData = getData(dataType, index, originRow);
//                                    String[] colName = checkTableforselectCols.split(",");
//                                    result = result + " - " + colName[index] + " : " + colData;
//                                }
//                                logger.error("ThreadID: {}{} - {} length: {}", Thread.currentThread().getId(), result, filterColName, rowColcnt);
//                                continue;
//                            }
//                        }
//                    }
//                } else {
//                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
//                    for (Row originRow : resultSet) {
//                        readLimiter.acquire(1);
//                        writeLimiter.acquire(1);
//
//                        if (checkTableforColSize) {
//                            int rowColcnt = GetRowColumnLength(originRow, filterColType, filterColIndex);
//                            String result = "";
//                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
//                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
//                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
//                                    Object colData = getData(dataType, index, originRow);
//                                    String[] colName = checkTableforselectCols.split(",");
//                                    result = result + " - " + colName[index] + " : " + colData;
//                                }
//                                logger.error("ThreadID: {}{} - {} length: {}", Thread.currentThread().getId(), result, filterColName, rowColcnt);
//                                continue;
//                            }
//                        }
//
//                        if (readCounter.incrementAndGet() % 1000 == 0) {
//                            logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
//                        }
//
//                    }
//                }
//
//                logger.info("ThreadID: {} Final Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
//                done = true;
//            } catch (Exception e) {
//                logger.error("Error occurred during Attempt#: {}", attempts, e);
//                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
//                        Thread.currentThread().getId(), min, max, attempts);
//            }
//        }
//    }
//
//    private int GetRowColumnLength(Row originRow, String filterColType, Integer filterColIndex) {
//        int sizeInMB = 0;
//        Object colData = getData(new MigrateDataType(filterColType), filterColIndex, originRow);
//        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
//        sizeInMB = colBytes.length;
//        if (sizeInMB > fieldGuardraillimitMB)
//            return sizeInMB;
//        return sizeInMB;
//    }

}
