package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import datastax.astra.migrate.cql.CqlHelper;
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
    protected AtomicLong errorCounter = new AtomicLong(0);

    protected CopyJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);
    }

    public static CopyJobSession getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(originSession, targetSession, sc);
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
            long errCnt = 0;
            try {
                ResultSet resultSet = cqlHelper.getOriginSession().execute(
                        cqlHelper.getPreparedStatement(CqlHelper.CQL.ORIGIN_SELECT)
                            .bind(cqlHelper.hasRandomPartitioner() ? min : min.longValueExact(),
                                    cqlHelper.hasRandomPartitioner() ? max : max.longValueExact())
                            .setConsistencyLevel(cqlHelper.getReadConsistencyLevel())
                            .setPageSize(cqlHelper.getFetchSizeInRows()));

                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (cqlHelper.getBatchSize() == 1 || cqlHelper.hasWriteTimestampFilter() || cqlHelper.isCounterTable()) {
                    for (Row originRow : resultSet) {
                        readLimiter.acquire(1);
                        readCnt++;
                        if (readCnt % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        // exclusion filter below
                        if (cqlHelper.hasFilterColumn()) {
                            String col = (String) cqlHelper.getData(cqlHelper.getFilterColType(), cqlHelper.getFilterColIndex(), originRow);
                            if (col.trim().equalsIgnoreCase(cqlHelper.getFilterColValue())) {
                                logger.warn("Skipping row and filtering out: {}", cqlHelper.getKey(originRow));
                                skipCnt++;
                                continue;
                            }
                        }
                        if (cqlHelper.hasWriteTimestampFilter()) {
                            // only process rows greater than writeTimeStampFilter
                            Long originWriteTimeStamp = cqlHelper.getLargestWriteTimeStamp(originRow);
                            if (originWriteTimeStamp < cqlHelper.getMinWriteTimeStampFilter()
                                    || originWriteTimeStamp > cqlHelper.getMaxWriteTimeStampFilter()) {
                                skipCnt++;
                                continue;
                            }
                        }
                        writeLimiter.acquire(1);

                        Row targetRow = null;
                        if (cqlHelper.isCounterTable()) {
                            ResultSet targetResultSet = cqlHelper.getTargetSession()
                                    .execute(cqlHelper.selectFromTargetByPK(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_SELECT_ORIGIN_BY_PK), originRow));
                            targetRow = targetResultSet.one();
                        }

                        BoundStatement bInsert = cqlHelper.bindInsertOneRow(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), originRow, targetRow);
                        if (null == bInsert) {
                            skipCnt++;
                            continue;
                        }
                        CompletionStage<AsyncResultSet> targetWriteResultSet = cqlHelper.getTargetSession().executeAsync(bInsert);
                        writeResults.add(targetWriteResultSet);
                        if (writeResults.size() > cqlHelper.getFetchSizeInRows()) {
                            writeCnt += iterateAndClearWriteResults(writeResults, 1);
                        }
                    }

                    // clear the write resultset
                    writeCnt += iterateAndClearWriteResults(writeResults, 1);
                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row originRow : resultSet) {
                        readLimiter.acquire(1);
                        readCnt++;
                        if (readCnt % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        if (cqlHelper.hasFilterColumn()) {
                            String colValue = (String) cqlHelper.getData(cqlHelper.getFilterColType(), cqlHelper.getFilterColIndex(), originRow);
                            if (colValue.trim().equalsIgnoreCase(cqlHelper.getFilterColValue())) {
                                logger.warn("Skipping row and filtering out: {}", cqlHelper.getKey(originRow));
                                skipCnt++;
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);
                        BoundStatement bInsert = cqlHelper.bindInsertOneRow(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), originRow, null);
                        if (null == bInsert) {
                            skipCnt++;
                            continue;
                        }
                        batchStatement = batchStatement.add(bInsert);

                        // if batch threshold is met, send the writes and clear the batch
                        if (batchStatement.size() >= cqlHelper.getBatchSize()) {
                            CompletionStage<AsyncResultSet> writeResultSet = cqlHelper.getTargetSession().executeAsync(batchStatement);
                            writeResults.add(writeResultSet);
                            batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                        }

                        if (writeResults.size() * cqlHelper.getBatchSize() > cqlHelper.getFetchSizeInRows()) {
                            writeCnt += iterateAndClearWriteResults(writeResults, cqlHelper.getBatchSize());
                        }
                    }

                    // clear the write resultset
                    writeCnt += iterateAndClearWriteResults(writeResults, cqlHelper.getBatchSize());

                    // if there are any pending writes because the batchSize threshold was not met, then write and clear them
                    if (batchStatement.size() > 0) {
                        CompletionStage<AsyncResultSet> writeResultSet = cqlHelper.getTargetSession().executeAsync(batchStatement);
                        writeResults.add(writeResultSet);
                        writeCnt += iterateAndClearWriteResults(writeResults, batchStatement.size());
                        batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
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
