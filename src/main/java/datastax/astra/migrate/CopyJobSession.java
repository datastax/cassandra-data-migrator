package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import datastax.astra.migrate.schema.TypeInfo;
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
    protected AtomicLong skippedCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);
    protected AtomicLong errorCounter = new AtomicLong(0);

    protected CopyJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc);
        filterData = Boolean.parseBoolean(sc.get("spark.origin.FilterData", "false"));
        filterColName = Util.getSparkPropOrEmpty(sc, "spark.origin.FilterColumn");
        filterColType = Util.getSparkPropOrEmpty(sc, "spark.origin.FilterColumnType");
        filterColIndex = Integer.parseInt(sc.get("spark.origin.FilterColumnIndex", "0"));
        filterColValue = Util.getSparkPropOrEmpty(sc, "spark.origin.FilterColumnValue");
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
        boolean done = false;
        int maxAttempts = maxRetries + 1;
        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
            long readCnt = 0;
            long writeCnt = 0;
            long skipCnt = 0;
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
                        readCnt++;
                        if (readCnt % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        if (filterData) {
                            String col = (String) getData(new TypeInfo(filterColType), filterColIndex, sourceRow);
                            if (col.trim().equalsIgnoreCase(filterColValue)) {
                                logger.warn("Skipping row and filtering out: {}", getKey(sourceRow, tableInfo));
                                skipCnt++;
                                continue;
                            }
                        }
                        if (writeTimeStampFilter) {
                            // only process rows greater than writeTimeStampFilter
                            Long sourceWriteTimeStamp = getLargestWriteTimeStamp(sourceRow);
                            if (sourceWriteTimeStamp < minWriteTimeStampFilter
                                    || sourceWriteTimeStamp > maxWriteTimeStampFilter) {
                                skipCnt++;
                                continue;
                            }
                        }
                        writeLimiter.acquire(1);

                        Row astraRow = null;
                        if (isCounterTable) {
                            ResultSet astraReadResultSet = astraSession
                                    .execute(selectFromAstra(astraSelectStatement, sourceRow));
                            astraRow = astraReadResultSet.one();
                        }

                        BoundStatement bInsert = bindInsert(astraInsertStatement, sourceRow, astraRow);
                        if (null == bInsert) {
                            skipCnt++;
                            continue;
                        }
                        CompletionStage<AsyncResultSet> astraWriteResultSet = astraSession.executeAsync(bInsert);
                        writeResults.add(astraWriteResultSet);
                        if (writeResults.size() > fetchSizeInRows) {
                            writeCnt += iterateAndClearWriteResults(writeResults, 1);
                        }
                    }

                    // clear the write resultset
                    writeCnt += iterateAndClearWriteResults(writeResults, 1);
                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        readCnt++;
                        if (readCnt % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        if (filterData) {
                            String colValue = (String) getData(new TypeInfo(filterColType), filterColIndex, sourceRow);
                            if (colValue.trim().equalsIgnoreCase(filterColValue)) {
                                logger.warn("Skipping row and filtering out: {}", getKey(sourceRow, tableInfo));
                                skipCnt++;
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);
                        BoundStatement bInsert = bindInsert(astraInsertStatement, sourceRow, null);
                        if (null == bInsert) {
                            skipCnt++;
                            continue;
                        }
                        batchStatement = batchStatement.add(bInsert);

                        // if batch threshold is met, send the writes and clear the batch
                        if (batchStatement.size() >= batchSize) {
                            CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                            writeResults.add(writeResultSet);
                            batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                        }

                        if (writeResults.size() * batchSize > fetchSizeInRows) {
                            writeCnt += iterateAndClearWriteResults(writeResults, batchSize);
                        }
                    }

                    // clear the write resultset
                    writeCnt += iterateAndClearWriteResults(writeResults, batchSize);

                    // if there are any pending writes because the batchSize threshold was not met, then write and clear them
                    if (batchStatement.size() > 0) {
                        CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                        writeResults.add(writeResultSet);
                        writeCnt += iterateAndClearWriteResults(writeResults, batchStatement.size());
                    }
                }

                readCounter.addAndGet(readCnt);
                writeCounter.addAndGet(writeCnt);
                skippedCounter.addAndGet(skipCnt);
                done = true;
            } catch (Exception e) {
                if (attempts == maxAttempts) {
                    readCounter.addAndGet(readCnt);
                    writeCounter.addAndGet(writeCnt);
                    skippedCounter.addAndGet(skipCnt);
                    errorCounter.addAndGet(readCnt - writeCnt - skipCnt);
                }
                logger.error("Error occurred during Attempt#: {}", attempts, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
                        Thread.currentThread().getId(), min, max, attempts);
                logger.error("Error stats Read#: {}, Wrote#: {}, Skipped#: {}, Error#: {}", readCnt, writeCnt, skipCnt, (readCnt - writeCnt - skipCnt));
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
        logger.info("{} Error Record Count: {}", msg, errorCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private int iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults, int incrementBy) throws Exception {
        int cnt = 0;
        for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
            //wait for the writes to complete for the batch. The Retry policy, if defined, should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
            cnt += incrementBy;
        }
        writeResults.clear();

        return cnt;
    }

}
