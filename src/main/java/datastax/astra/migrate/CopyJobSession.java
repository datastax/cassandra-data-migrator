package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class CopyJobSession extends AbstractJobSession {

    private static CopyJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong skippedCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    protected CopyJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc);
        filterData = Boolean.parseBoolean(sc.get("spark.origin.FilterData", "false"));
        filterColName = sc.get("spark.origin.FilterColumn");
        filterColType = sc.get("spark.origin.FilterColumnType");
        filterColIndex = Integer.parseInt(sc.get("spark.origin.FilterColumnIndex", "0"));
        filterColValue = sc.get("spark.origin.FilterColumnValue");
    }

    public static CopyJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(sourceSession, astraSession, sc);
                }
            }
        }

        return copyJobSession;
    }

    public void getDataAndInsert(BigInteger min, BigInteger max) {
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
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

                        if (filterData) {
                            String col = (String) getData(new MigrateDataType(filterColType), filterColIndex, sourceRow);
                            if (col.trim().equalsIgnoreCase(filterColValue)) {
                                logger.warn("Row larger than 10 MB found filtering out: " + getKey(sourceRow));
                                continue;
                            }
                        }
                        if (writeTimeStampFilter) {
                            // only process rows greater than writeTimeStampFilter
                            Long sourceWriteTimeStamp = getLargestWriteTimeStamp(sourceRow);
                            if (sourceWriteTimeStamp < minWriteTimeStampFilter
                                    || sourceWriteTimeStamp > maxWriteTimeStampFilter) {
                                readCounter.incrementAndGet();
                                skippedCounter.incrementAndGet();
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts(false);
                        }
                        Row astraRow = null;
                        if (isCounterTable) {
                            ResultSet astraReadResultSet = astraSession
                                    .execute(selectFromAstra(astraSelectStatement, sourceRow));
                            astraRow = astraReadResultSet.one();
                        }

                        CompletionStage<AsyncResultSet> astraWriteResultSet = astraSession
                                .executeAsync(bindInsert(astraInsertStatement, sourceRow, astraRow));
                        writeResults.add(astraWriteResultSet);
                        if (writeResults.size() > 1000) {
                            iterateAndClearWriteResults(writeResults, 1);
                        }
                    }

                    // clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults, 1);
                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        if (filterData) {
                            String colValue = (String) getData(new MigrateDataType(filterColType), filterColIndex, sourceRow);
                            if (colValue.trim().equalsIgnoreCase(filterColValue)) {
                                logger.warn("Row larger than 10 MB found filtering out: " + getKey(sourceRow));
                                continue;
                            }
                        }

                        batchStatement = batchStatement.add(bindInsert(astraInsertStatement, sourceRow, null));

                        // if batch threshold is met, send the writes and clear the batch
                        if (batchStatement.size() >= batchSize) {
                            CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                            writeResults.add(writeResultSet);
                            batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                        }

                        if (writeResults.size() * batchSize > 1000) {
                            iterateAndClearWriteResults(writeResults, batchSize);
                        }
                    }

                    // clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults, batchSize);

                    // if there are any pending writes because the batchSize threshold was not met, then write and clear them
                    if (batchStatement.size() > 0) {
                        CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                        writeResults.add(writeResultSet);
                        iterateAndClearWriteResults(writeResults, batchStatement.size());
                        batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    }
                }

                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: {}", retryCount, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Retry# {}",
                        Thread.currentThread().getId(), min, max, retryCount);
            }
        }
    }

    public synchronized void printCounts(boolean isFinal) {
        String msg = "ThreadID: " + Thread.currentThread().getId();
        if (isFinal) {
            msg += " Final";
            logger.info("################################################################################################");
        }
        logger.info("{} Read Record Count: {}", msg, readCounter.get());
        logger.info("{} Skipped Record Count: {}", msg, skippedCounter.get());
        logger.info("{} Write Record Count: {}", msg, writeCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private void iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults, int incrementBy) throws Exception {
        for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
            //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
            writeCounter.addAndGet(incrementBy);
        }
        writeResults.clear();
    }

}