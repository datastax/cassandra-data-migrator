package com.datastax.cdm.job;

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.TargetSelectByPKStatement;
import com.datastax.cdm.cql.statement.TargetUpsertStatement;
import com.datastax.cdm.feature.Guardrail;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class CopyJobSession extends AbstractJobSession<SplitPartitions.Partition> {

    private static CopyJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong skippedCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);
    protected AtomicLong errorCounter = new AtomicLong(0);

    private TargetUpsertStatement targetUpsertStatement;
    private TargetSelectByPKStatement targetSelectByPKStatement;
    private final PKFactory pkFactory;
    private final boolean isCounterTable;
    private Integer batchSize;
    private final Integer fetchSize;

    private BatchStatement batch;

    protected CopyJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);

        pkFactory = this.originSession.getPKFactory();
        isCounterTable = this.originSession.getCqlTable().isCounterTable();
        fetchSize = this.originSession.getCqlTable().getFetchSizeInRows();
        batchSize = this.originSession.getCqlTable().getBatchSize();

        batch = BatchStatement.newInstance(BatchType.UNLOGGED);

        logger.info("CQL -- origin select: {}",this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
        logger.info("CQL -- target select: {}",this.targetSession.getTargetSelectByPKStatement().getCQL());
        logger.info("CQL -- target upsert: {}",this.targetSession.getTargetUpsertStatement().getCQL());
    }

    @Override
    public void processSlice(SplitPartitions.Partition slice) {
        this.getDataAndInsert(slice.getMin(), slice.getMax());
    }

    public void getDataAndInsert(BigInteger min, BigInteger max) {
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min,max));
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        boolean done = false;
        int maxAttempts = maxRetries + 1;
        String guardrailCheck;
        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
            long readCnt = 0;
            long flushedWriteCnt = 0;
            long skipCnt = 0;
            long errCnt = 0;
            long unflushedWrites = 0;
            try {
                OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = this.originSession.getOriginSelectByPartitionRangeStatement();
                targetUpsertStatement = this.targetSession.getTargetUpsertStatement();
                targetSelectByPKStatement = this.targetSession.getTargetSelectByPKStatement();
                ResultSet resultSet = originSelectByPartitionRangeStatement.execute(originSelectByPartitionRangeStatement.bind(min, max));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<>();

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
                        if (guardrailEnabled) {
                            guardrailCheck = guardrailFeature.guardrailChecks(r);
                            if (guardrailCheck != null && guardrailCheck != Guardrail.CLEAN_CHECK) {
                                logger.error("Guardrails failed for PrimaryKey {}; {}", r.getPk(), guardrailCheck);
                                skipCnt++;
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);

                        BoundStatement boundUpsert = bind(r);
                        if (null == boundUpsert) {
                            skipCnt++; // TODO: this previously skipped, why not errCnt?
                            continue;
                        }

                        writeAsync(writeResults, boundUpsert);
                        unflushedWrites++;

                        if (unflushedWrites > fetchSize) {
                            flushAndClearWrites(writeResults);
                            flushedWriteCnt += unflushedWrites;
                            unflushedWrites = 0;
                        }
                    }
                }

                flushAndClearWrites(writeResults);
                flushedWriteCnt += unflushedWrites;

                readCounter.addAndGet(readCnt);
                writeCounter.addAndGet(flushedWriteCnt);
                skippedCounter.addAndGet(skipCnt);
                done = true;

            } catch (Exception e) {
                if (attempts == maxAttempts) {
                    readCounter.addAndGet(readCnt);
                    writeCounter.addAndGet(flushedWriteCnt);
                    skippedCounter.addAndGet(skipCnt);
                    errorCounter.addAndGet(readCnt - flushedWriteCnt - skipCnt);
                }
                logger.error("Error occurred during Attempt#: {}", attempts, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
                        Thread.currentThread().getId(), min, max, attempts);
                logger.error("Error stats Read#: {}, Wrote#: {}, Skipped#: {}, Error#: {}", readCnt, flushedWriteCnt, skipCnt, (readCnt - flushedWriteCnt - skipCnt));
            }
        }
    }

    @Override
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

    private void flushAndClearWrites(Collection<CompletionStage<AsyncResultSet>> writeResults) throws Exception {
        if (batch.size() > 0) {
            writeResults.add(targetUpsertStatement.executeAsync(batch));
        }
        for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
            //wait for the writes to complete for the batch. The Retry policy, if defined, should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
        }
        writeResults.clear();
    }

    private BoundStatement bind(Record r) {
        if (isCounterTable) {
            readLimiterTarget.acquire(1);
            Record targetRecord = targetSelectByPKStatement.getRecord(r.getPk());
            if (null != targetRecord) {
                r.setTargetRow(targetRecord.getTargetRow());
            }
        }
        return targetUpsertStatement.bindRecord(r);
    }

    private void writeAsync(Collection<CompletionStage<AsyncResultSet>> writeResults, BoundStatement boundUpsert) {
        if (batchSize > 1) {
            batch = batch.add(boundUpsert);
            if (batch.size() >= batchSize) {
                writeResults.add(targetUpsertStatement.executeAsync(batch));
                batch = BatchStatement.newInstance(BatchType.UNLOGGED);
            }
        }
        else {
            writeResults.add(targetUpsertStatement.executeAsync(boundUpsert));
        }
    }

}
