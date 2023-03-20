package datastax.astra.migrate;

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

    protected OriginCountJobSession(CqlSession sourceSession, SparkConf sc) {
        super(sc);
        this.sourceSession = sourceSession;
        batchSize = new Integer(sc.get("spark.batchSize", "1"));
        printStatsAfter = new Integer(sc.get("spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sc.get("spark.readRateLimit", "20000")));
        sourceKeyspaceTable = sc.get("spark.origin.keyspaceTable");

        hasRandomPartitioner = Boolean.parseBoolean(sc.get("spark.origin.hasRandomPartitioner", "false"));
        isCounterTable = Boolean.parseBoolean(sc.get("spark.counterTable", "false"));

        checkTableforColSize = Boolean.parseBoolean(sc.get("spark.origin.checkTableforColSize", "false"));
        checkTableforselectCols = sc.get("spark.origin.checkTableforColSize.cols");
        checkTableforColSizeTypes = getTypes(sc.get("spark.origin.checkTableforColSize.cols.types"));
        filterColName = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_COLUMN_NAME);
        filterColType = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_COLUMN_TYPE); // TODO: this is a string, but should be MigrationDataType?
        filterColIndex = Integer.parseInt(sc.get("spark.origin.FilterColumnIndex", "0"));
        fieldGuardraillimitMB = Integer.parseInt(sc.get("spark.fieldGuardraillimitMB", "0"));

        String partionKey = sc.get("spark.query.cols.partitionKey");
        idColTypes = getTypes(sc.get("spark.query.cols.id.types"));

        String selectCols = sc.get("spark.query.cols.select");
        String updateSelectMappingStr = sc.get("spark.counterTable.cql.index", "0");
        for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
            updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
        }
        String sourceSelectCondition = sc.get("spark.query.cols.select.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");
    }

    public static OriginCountJobSession getInstance(CqlSession sourceSession, SparkConf sparkConf) {
        if (originCountJobSession == null) {
            synchronized (OriginCountJobSession.class) {
                if (originCountJobSession == null) {
                    originCountJobSession = new OriginCountJobSession(sourceSession, sparkConf);
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
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(hasRandomPartitioner ?
                                min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact())
                        .setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));

                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);

                        if (checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(sourceRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, sourceRow);
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
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);

                        if (checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(sourceRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > fieldGuardraillimitMB * 1048576) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, sourceRow);
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

    private int GetRowColumnLength(Row sourceRow, String filterColType, Integer filterColIndex) {
        int sizeInMB = 0;
        Object colData = getData(new MigrateDataType(filterColType), filterColIndex, sourceRow);
        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
        sizeInMB = colBytes.length;
        if (sizeInMB > fieldGuardraillimitMB)
            return sizeInMB;
        return sizeInMB;
    }

}
