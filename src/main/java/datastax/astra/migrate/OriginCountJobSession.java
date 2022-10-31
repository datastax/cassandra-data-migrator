package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class OriginCountJobSession extends BaseJobSession{
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private static OriginCountJobSession originCountJobSession;
    protected AtomicLong readCounter = new AtomicLong(0);
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();
    protected Boolean checkTableforColSize;
    protected String checkTableforselectCols;
    protected String filterColName;
    protected String filterColType;
    protected Integer filterColIndex;
    protected List<MigrateDataType> checkTableforColSizeTypes = new ArrayList<MigrateDataType>();
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

    protected OriginCountJobSession(CqlSession sourceSession, SparkConf sparkConf) {
        this.sourceSession = sourceSession;
        batchSize = new Integer(sparkConf.get("spark.batchSize", "1"));
        printStatsAfter = new Integer(sparkConf.get("spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.readRateLimit", "20000")));
        sourceKeyspaceTable = sparkConf.get("spark.source.keyspaceTable");

        hasRandomPartitioner = Boolean.parseBoolean(sparkConf.get("spark.source.hasRandomPartitioner", "false"));
        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.source.counterTable", "false"));

        checkTableforColSize = Boolean.parseBoolean(sparkConf.get("spark.source.checkTableforColSize", "false"));
        checkTableforselectCols = sparkConf.get("spark.source.checkTableforColSize.cols");
        checkTableforColSizeTypes = getTypes(sparkConf.get("spark.source.checkTableforColSize.cols.types"));
        filterColName = sparkConf.get("spark.source.FilterColumn");
        filterColType = sparkConf.get("spark.source.FilterColumnType");
        filterColIndex =  Integer.parseInt(sparkConf.get("spark.source.FilterColumnIndex", "0"));

        String partionKey = sparkConf.get("spark.query.cols.partitionKey");
        idColTypes = getTypes(sparkConf.get("spark.query.cols.id.types"));

        String selectCols = sparkConf.get("spark.query.cols.select");
        String updateSelectMappingStr = sparkConf.get("spark.source.counterTable.update.select.index", "0");
        for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
            updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
        }
        String sourceSelectCondition = sparkConf.get("spark.query.cols.select.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");

    }

    public void getData(BigInteger min, BigInteger max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(hasRandomPartitioner ? min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact()));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);

                        if(checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(sourceRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > 1024 * 1024 * 10) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, sourceRow);
                                    String[] colName = checkTableforselectCols.split(",");
                                    result = result + " - " + colName[index] + " : " + colData;
                                }
                                logger.error("ThreadID: " + Thread.currentThread().getId() + result + " - " + filterColName + " length: " + rowColcnt);
                                continue;
                            }
                        }
                    }

                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);

                        if(checkTableforColSize) {
                            int rowColcnt = GetRowColumnLength(sourceRow, filterColType, filterColIndex);
                            String result = "";
                            if (rowColcnt > 1024 * 1024 * 10) {
                                for (int index = 0; index < checkTableforColSizeTypes.size(); index++) {
                                    MigrateDataType dataType = checkTableforColSizeTypes.get(index);
                                    Object colData = getData(dataType, index, sourceRow);
                                    String[] colName = checkTableforselectCols.split(",");
                                    result = result + " - " + colName[index] + " : " + colData;
                                }
                                logger.error("ThreadID: " + Thread.currentThread().getId() + result + " - " + filterColName + " length: " + rowColcnt);
                                continue;
                            }
                        }

                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }

                    }
                }


                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

    }

    private int GetRowColumnLength(Row sourceRow, String filterColType, Integer filterColIndex) {
        int i = 0;
        Object colData = getData(new MigrateDataType(filterColType), filterColIndex, sourceRow);
        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
        i = colBytes.length;
        if (i > 1024*1024*10)
            return i;
        return i;
    }

}
