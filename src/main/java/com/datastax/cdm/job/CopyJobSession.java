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

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.TargetSelectByPKStatement;
import com.datastax.cdm.cql.statement.TargetUpsertStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.Guardrail;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

public class CopyJobSession extends AbstractJobSession<SplitPartitions.Partition> {

	private final PKFactory pkFactory;
	private final boolean isCounterTable;
	private final Integer fetchSize;
	private final Integer batchSize;
	public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	private TargetUpsertStatement targetUpsertStatement;
	private TargetSelectByPKStatement targetSelectByPKStatement;

	protected CopyJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
		super(originSession, targetSession, sc);
		this.jobCounter.setRegisteredTypes(JobCounter.CounterType.READ, JobCounter.CounterType.WRITE,
				JobCounter.CounterType.SKIPPED, JobCounter.CounterType.ERROR, JobCounter.CounterType.UNFLUSHED);

		pkFactory = this.originSession.getPKFactory();
		isCounterTable = this.originSession.getCqlTable().isCounterTable();
		fetchSize = this.originSession.getCqlTable().getFetchSizeInRows();
		batchSize = this.originSession.getCqlTable().getBatchSize();

		logger.info("CQL -- origin select: {}", this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
		logger.info("CQL -- target select: {}", this.targetSession.getTargetSelectByPKStatement().getCQL());
		logger.info("CQL -- target upsert: {}", this.targetSession.getTargetUpsertStatement().getCQL());
	}

	@Override
	public void processSlice(SplitPartitions.Partition slice) {
		this.getDataAndInsert(slice.getMin(), slice.getMax());
	}

	public synchronized void initCdmRun(Collection<SplitPartitions.Partition> parts, TrackRun trackRunFeature) {
		this.trackRunFeature = trackRunFeature;
		if (trackRun)
			trackRunFeature.initCdmRun(parts, TrackRun.RUN_TYPE.MIGRATE);
	}

	public void getDataAndInsert(BigInteger min, BigInteger max) {
		ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
		logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
		if (trackRun)
			trackRunFeature.updateCdmRun(min, TrackRun.RUN_STATUS.STARTED);

		BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
		boolean done = false;
		int maxAttempts = maxRetries + 1;
		String guardrailCheck;
		for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
			jobCounter.threadReset();

			try {
				OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = this.originSession
						.getOriginSelectByPartitionRangeStatement();
				targetUpsertStatement = this.targetSession.getTargetUpsertStatement();
				targetSelectByPKStatement = this.targetSession.getTargetSelectByPKStatement();
				ResultSet resultSet = originSelectByPartitionRangeStatement
						.execute(originSelectByPartitionRangeStatement.bind(min, max));
				Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<>();

				for (Row originRow : resultSet) {
					rateLimiterOrigin.acquire(1);
					jobCounter.threadIncrement(JobCounter.CounterType.READ);

					Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
					if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
						jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED);
						continue;
					}

					for (Record r : pkFactory.toValidRecordList(record)) {
						if (guardrailEnabled) {
							guardrailCheck = guardrailFeature.guardrailChecks(r);
							if (guardrailCheck != null && guardrailCheck != Guardrail.CLEAN_CHECK) {
								logger.error("Guardrails failed for PrimaryKey {}; {}", r.getPk(), guardrailCheck);
								jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED);
								continue;
							}
						}

						BoundStatement boundUpsert = bind(r);
						if (null == boundUpsert) {
							jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED); // TODO: this previously
																						// skipped, why not errCnt?
							continue;
						}

						rateLimiterTarget.acquire(1);
						batch = writeAsync(batch, writeResults, boundUpsert);
						jobCounter.threadIncrement(JobCounter.CounterType.UNFLUSHED);

						if (jobCounter.getCount(JobCounter.CounterType.UNFLUSHED) > fetchSize) {
							flushAndClearWrites(batch, writeResults);
							jobCounter.threadIncrement(JobCounter.CounterType.WRITE,
									jobCounter.getCount(JobCounter.CounterType.UNFLUSHED));
							jobCounter.threadReset(JobCounter.CounterType.UNFLUSHED);
						}
					}
				}

				flushAndClearWrites(batch, writeResults);
				jobCounter.threadIncrement(JobCounter.CounterType.WRITE,
						jobCounter.getCount(JobCounter.CounterType.UNFLUSHED));
				jobCounter.threadReset(JobCounter.CounterType.UNFLUSHED);
				done = true;
				if (trackRun)
					trackRunFeature.updateCdmRun(min, TrackRun.RUN_STATUS.PASS);

			} catch (Exception e) {
				if (attempts == maxAttempts) {
					jobCounter.threadIncrement(JobCounter.CounterType.ERROR,
							jobCounter.getCount(JobCounter.CounterType.READ)
									- jobCounter.getCount(JobCounter.CounterType.WRITE)
									- jobCounter.getCount(JobCounter.CounterType.SKIPPED));
					logPartitionsInFile(partitionFileOutput, min, max);
				}
				logger.error("Error occurred during Attempt#: {}", attempts, e);
				logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
						Thread.currentThread().getId(), min, max, attempts);
				logger.error("Error stats " + jobCounter.getThreadCounters(false));
				if (trackRun)
					trackRunFeature.updateCdmRun(min, TrackRun.RUN_STATUS.FAIL);
			} finally {
				jobCounter.globalIncrement();
				printCounts(false);
			}
		}
	}

	private void flushAndClearWrites(BatchStatement batch, Collection<CompletionStage<AsyncResultSet>> writeResults)
			throws Exception {
		if (batch.size() > 0) {
			writeResults.add(targetUpsertStatement.executeAsync(batch));
		}
		for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
			// wait for the writes to complete for the batch. The Retry policy, if defined,
			// should retry the write on timeouts.
			writeResult.toCompletableFuture().get().one();
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
				return BatchStatement.newInstance(BatchType.UNLOGGED);
			}
			return batch;
		} else {
			writeResults.add(targetUpsertStatement.executeAsync(boundUpsert));
			return batch;
		}
	}

}
