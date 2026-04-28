/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.job;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.TargetSelectByPKStatement;
import com.datastax.cdm.cql.statement.TargetUpsertStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class CopyJobSession extends AbstractJobSession<PartitionRange> {

    private final PKFactory pkFactory;
    private final boolean isCounterTable;
    private final Integer fetchSize;
    private final Integer batchSize;
    private final Integer flushThreshold;

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement;
    private TargetUpsertStatement targetUpsertStatement;
    private TargetSelectByPKStatement targetSelectByPKStatement;

    protected CopyJobSession(CqlSession originSession, CqlSession targetSession, PropertyHelper propHelper) {
        super(originSession, targetSession, propHelper);
        pkFactory = this.originSession.getPKFactory();
        isCounterTable = this.originSession.getCqlTable().isCounterTable();
        fetchSize = this.originSession.getCqlTable().getFetchSizeInRows();
        batchSize = this.originSession.getCqlTable().getBatchSize();
        this.flushThreshold = Math.min(fetchSize, Math.max(batchSize * 10, 100));
        originSelectByPartitionRangeStatement = this.originSession.getOriginSelectByPartitionRangeStatement();
        targetSelectByPKStatement = this.targetSession.getTargetSelectByPKStatement();
        targetUpsertStatement = this.targetSession.getTargetUpsertStatement();

        logger.info("CQL -- origin select: {}", this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
        logger.info("CQL -- target select: {}", this.targetSession.getTargetSelectByPKStatement().getCQL());
        logger.info("CQL -- target upsert: {}", this.targetSession.getTargetUpsertStatement().getCQL());
        logger.info("PARAM -- Flush threshold: {} (fetchSize: {}, batchSize: {})", flushThreshold, fetchSize,
                batchSize);
    }

    protected void processPartitionRange(PartitionRange range) {
        BigInteger min = range.getMin(), max = range.getMax();
        BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED)
                .setConsistencyLevel(this.targetSession.getCqlTable().getWriteConsistencyLevel())
                .setTimeout(Duration.ofSeconds(10));

        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        if (null != trackRunFeature)
            trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.STARTED, "");

        JobCounter jobCounter = range.getJobCounter();

        try {
            ResultSet resultSet = originSelectByPartitionRangeStatement
                    .execute(originSelectByPartitionRangeStatement.bind(min, max));
            Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<>();

            for (Row originRow : resultSet) {
                rateLimiterOrigin.acquire(1);
                jobCounter.increment(JobCounter.CounterType.READ);

                Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                    jobCounter.increment(JobCounter.CounterType.SKIPPED);
                    continue;
                }

                for (Record r : pkFactory.toValidRecordList(record)) {
                    BoundStatement boundUpsert = bind(r);
                    r.setOriginRow(null);
                    if (null == boundUpsert) {
                        jobCounter.increment(JobCounter.CounterType.SKIPPED);
                        continue;
                    }

                    rateLimiterTarget.acquire(1);
                    batch = writeAsync(batch, writeResults, boundUpsert);
                    jobCounter.increment(JobCounter.CounterType.UNFLUSHED);

                    if (jobCounter.getCount(JobCounter.CounterType.UNFLUSHED) >= flushThreshold) {
                        flushAndClearWrites(batch, writeResults);
                        jobCounter.increment(JobCounter.CounterType.WRITE,
                                jobCounter.getCount(JobCounter.CounterType.UNFLUSHED, true));
                        jobCounter.reset(JobCounter.CounterType.UNFLUSHED);
                    }
                }
            }

            flushAndClearWrites(batch, writeResults);
            jobCounter.increment(JobCounter.CounterType.WRITE,
                    jobCounter.getCount(JobCounter.CounterType.UNFLUSHED, true));
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED);
            jobCounter.reset(JobCounter.CounterType.UNFLUSHED);
            jobCounter.flush();
            if (null != trackRunFeature) {
                trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.PASS, jobCounter.getMetrics());
            }
        } catch (Exception e) {
            jobCounter.increment(JobCounter.CounterType.ERROR,
                    jobCounter.getCount(JobCounter.CounterType.READ, true)
                            - jobCounter.getCount(JobCounter.CounterType.WRITE, true)
                            - jobCounter.getCount(JobCounter.CounterType.SKIPPED, true));
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max, e);
            logger.error("Error stats " + jobCounter.getMetrics(true));
            jobCounter.flush();
            if (null != trackRunFeature) {
                trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.FAIL, jobCounter.getMetrics());
            }
        } finally {
            ThreadContext.remove(THREAD_CONTEXT_LABEL);
        }
    }

    private void flushAndClearWrites(BatchStatement batch, Collection<CompletionStage<AsyncResultSet>> writeResults) {
        if (batch.size() > 0) {
            writeResults.add(targetUpsertStatement.executeAsync(batch));
        }

        // Process completed futures immediately to release memory early
        List<CompletionStage<AsyncResultSet>> remaining = new ArrayList<>();
        for (CompletionStage<AsyncResultSet> future : writeResults) {
            // Use getNow() to check completion
            AsyncResultSet result = future.toCompletableFuture().getNow(null);
            if (result == null) {
                // Future not complete yet, keep it for final wait
                remaining.add(future);
            } else {
                result.one();
                // Future can now be GC'd after this iteration
            }
        }

        // Wait for any remaining incomplete futures (small subset of the original collection)
        for (CompletionStage<AsyncResultSet> future : remaining) {
            try {
                future.toCompletableFuture().join().one();
            } catch (Exception e) {
                throw new RuntimeException("Async write operation failed during final flush", e);
            }
        }

        writeResults.clear();
    }

    private BoundStatement bind(Record r) {
        if (isCounterTable) {
            rateLimiterTarget.acquire(1);
            Record targetRecord = targetSelectByPKStatement.getRecord(r.getPk());
            if (null != targetRecord) {
                r.setTargetRow(targetRecord.getTargetRow());
            }
        }
        return targetUpsertStatement.bindRecord(r);
    }

    private BatchStatement writeAsync(BatchStatement batch, Collection<CompletionStage<AsyncResultSet>> writeResults,
            BoundStatement boundUpsert) {
        if (batchSize > 1) {
            batch = batch.add(boundUpsert);
            if (batch.size() >= batchSize) {
                writeResults.add(targetUpsertStatement.executeAsync(batch));
                return BatchStatement.newInstance(BatchType.UNLOGGED)
                        .setConsistencyLevel(this.targetSession.getCqlTable().getWriteConsistencyLevel())
                        .setTimeout(Duration.ofSeconds(10));
            }
            return batch;
        } else {
            writeResults.add(targetUpsertStatement.executeAsync(boundUpsert));
            return batch;
        }
    }

}
