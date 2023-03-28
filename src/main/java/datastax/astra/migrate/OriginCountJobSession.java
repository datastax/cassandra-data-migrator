package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.properties.KnownProperties;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class OriginCountJobSession extends BaseJobSession {
    private static OriginCountJobSession originCountJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();
    protected Boolean checkTableforColSize;
    protected String checkTableforselectCols;
    protected Integer fieldGuardraillimitMB;
    protected List<MigrateDataType> checkTableforColSizeTypes = new ArrayList<MigrateDataType>();

    protected OriginCountJobSession(CqlSession originSession, SparkConf sc) {
        super(sc);
        this.originSessionSession = originSession;
        batchSize = new Integer(sc.get(KnownProperties.SPARK_BATCH_SIZE, "1"));
        printStatsAfter = new Integer(sc.get(KnownProperties.SPARK_STATS_AFTER, "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sc.get(KnownProperties.SPARK_LIMIT_READ, "20000")));
        originKeyspaceTable = sc.get(KnownProperties.ORIGIN_KEYSPACE_TABLE);

        hasRandomPartitioner = Boolean.parseBoolean(sc.get(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER, "false"));
        isCounterTable = Boolean.parseBoolean(sc.get(KnownProperties.ORIGIN_IS_COUNTER, "false"));

        checkTableforColSize = Boolean.parseBoolean(sc.get(KnownProperties.ORIGIN_CHECK_COLSIZE_ENABLED, "false"));
        checkTableforselectCols = sc.get(KnownProperties.ORIGIN_CHECK_COLSIZE_COLUMN_NAMES);
        checkTableforColSizeTypes = getTypes(sc.get(KnownProperties.ORIGIN_CHECK_COLSIZE_COLUMN_TYPES));
        filterColName = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_FILTER_COLUMN_NAME);
        filterColType = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_FILTER_COLUMN_TYPE);
        filterColIndex = Integer.parseInt(sc.get(KnownProperties.ORIGIN_FILTER_COLUMN_INDEX, "0"));
        fieldGuardraillimitMB = Integer.parseInt(sc.get(KnownProperties.FIELD_GUARDRAIL_MB, "0"));

        String partionKey = sc.get(KnownProperties.ORIGIN_PARTITION_KEY);
        idColTypes = getTypes(sc.get(KnownProperties.TARGET_PRIMARY_KEY_TYPES));

        String selectCols = sc.get(KnownProperties.ORIGIN_COLUMN_NAMES);
        String updateSelectMappingStr = sc.get(KnownProperties.ORIGIN_COUNTER_INDEXES, "0");
        for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
            updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
        }
        String originSelectCondition = sc.get(KnownProperties.ORIGIN_FILTER_CONDITION, "");
        // TODO: AbstractJobSession has some checks to ensure AND is added to the condition
        originSelectStatement = originSession.prepare(
                "select " + selectCols + " from " + originKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + originSelectCondition + " ALLOW FILTERING");
    }

    public static OriginCountJobSession getInstance(CqlSession originSession, SparkConf sparkConf) {
        if (originCountJobSession == null) {
            synchronized (OriginCountJobSession.class) {
                if (originCountJobSession == null) {
                    originCountJobSession = new OriginCountJobSession(originSession, sparkConf);
                }
            }
        }

        return originCountJobSession;
    }

    public void getData(BigInteger min, BigInteger max) {
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        boolean done = false;
        int maxAttempts = maxRetries + 1;
        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
            try {
                ResultSet resultSet = originSessionSession.execute(originSelectStatement.bind(hasRandomPartitioner ?
                                min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact())
                        .setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));

                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
                    for (Row originRow : resultSet) {
                        readLimiter.acquire(1);

                        if (checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(originRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, originRow);
                                    String[] colName = checkTableforselectCols.split(",");
                                    result = result + " - " + colName[index] + " : " + colData;
                                }
                                logger.error("ThreadID: {}{} - {} length: {}", Thread.currentThread().getId(), result, filterColName, rowColcnt);
                                continue;
                            }
                        }
                    }
                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row originRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);

                        if (checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(originRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, originRow);
                                    String[] colName = checkTableforselectCols.split(",");
                                    result = result + " - " + colName[index] + " : " + colData;
                                }
                                logger.error("ThreadID: {}{} - {} length: {}", Thread.currentThread().getId(), result, filterColName, rowColcnt);
                                continue;
                            }
                        }

                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
                        }

                    }
                }

                logger.info("ThreadID: {} Final Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
                done = true;
            } catch (Exception e) {
                logger.error("Error occurred during Attempt#: {}", attempts, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
                        Thread.currentThread().getId(), min, max, attempts);
            }
        }
    }

    private int GetRowColumnLength(Row originRow, String filterColType, Integer filterColIndex) {
        int sizeInMB = 0;
        Object colData = getData(new MigrateDataType(filterColType), filterColIndex, originRow);
        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
        sizeInMB = colBytes.length;
        if (sizeInMB > fieldGuardraillimitMB)
            return sizeInMB;
        return sizeInMB;
    }

}
