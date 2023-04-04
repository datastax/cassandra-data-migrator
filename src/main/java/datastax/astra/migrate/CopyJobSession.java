package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import datastax.astra.migrate.cql.PKFactory;
import datastax.astra.migrate.cql.Record;
import datastax.astra.migrate.cql.statements.OriginSelectByPartitionRangeStatement;
import datastax.astra.migrate.cql.statements.TargetInsertStatement;
import datastax.astra.migrate.cql.statements.TargetSelectByPKStatement;
import datastax.astra.migrate.cql.statements.TargetUpdateStatement;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
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
        boolean batching = false;
        int maxAttempts = maxRetries + 1;
        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
            long readCnt = 0;
            long writeCnt = 0;
            long skipCnt = 0;
            long errCnt = 0;
            try {
                PKFactory pkFactory = cqlHelper.getPKFactory();
                OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = cqlHelper.getOriginSelectByPartitionRangeStatement();
                ResultSet resultSet = originSelectByPartitionRangeStatement.execute(originSelectByPartitionRangeStatement.bind(min, max));

                TargetInsertStatement targetInsertStatement = cqlHelper.getTargetInsertStatement();
                TargetUpdateStatement targetUpdateStatement = cqlHelper.getTargetUpdateStatement();
                TargetSelectByPKStatement targetSelectByPKStatement = cqlHelper.getTargetSelectByPKStatement();

                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                batching = !(cqlHelper.getBatchSize() == 1 || cqlHelper.hasWriteTimestampFilter() || cqlHelper.isCounterTable());
                BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED); // this may not be used

                boolean isCounterTable = cqlHelper.isCounterTable();
                CompletionStage<AsyncResultSet> writeResultSet;

                for (Row originRow : resultSet) {
                    readLimiter.acquire(1);
                    readCnt++;
                    if (readCnt % printStatsAfter == 0) {
                        printCounts(false);
                    }

                    Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                    if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                        skipCnt++;
                        continue;
                    }

                    for (Record r : pkFactory.toValidRecordList(record)) {
                        writeLimiter.acquire(1);

                        BoundStatement boundUpsert;
                        if (isCounterTable) {
                            Record targetRecord = targetSelectByPKStatement.getRecord(r.getPk());
                            if (null != targetRecord) {
                                r.setTargetRow(targetRecord.getTargetRow());
                            }
                            boundUpsert = targetUpdateStatement.bindRecord(r);
                        }
                        else {
                            boundUpsert = targetInsertStatement.bindRecord(r);
                        }

                        if (null == boundUpsert) {
                            skipCnt++; // TODO: this previously skipped, why not errCnt?
                            continue;
                        }

                        if (batching) {
                            batch = batch.add(boundUpsert);
                            if (batch.size() >= cqlHelper.getBatchSize()) {
                                writeResultSet = isCounterTable ? targetUpdateStatement.executeAsync(batch) : targetInsertStatement.executeAsync(batch);
                                writeResults.add(writeResultSet);
                                batch = BatchStatement.newInstance(BatchType.UNLOGGED);
                            }

                            if (writeResults.size() * cqlHelper.getBatchSize() > cqlHelper.getFetchSizeInRows()) {
                                writeCnt += iterateAndClearWriteResults(writeResults, cqlHelper.getBatchSize());
                            }
                        }
                        else {
                            writeResultSet = isCounterTable ? targetUpdateStatement.executeAsync(boundUpsert) : targetInsertStatement.executeAsync(boundUpsert);
                            writeResults.add(writeResultSet);
                            if (writeResults.size() > cqlHelper.getFetchSizeInRows()) {
                                writeCnt += iterateAndClearWriteResults(writeResults, 1);
                            }
                        }
                    }
                }

                // Flush pending writes
                if (batching) {
                    if (batch.size() > 0) {
                        writeResultSet = isCounterTable ? targetUpdateStatement.executeAsync(batch) : targetInsertStatement.executeAsync(batch);
                        writeResults.add(writeResultSet);
                        writeCnt += iterateAndClearWriteResults(writeResults, batch.size());
                    }
                }
                else {
                    // clear the write resultset
                    writeCnt += iterateAndClearWriteResults(writeResults, 1);
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
