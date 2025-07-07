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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.TargetSelectByPKStatement;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.data.DataUtility;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.ConstantColumns;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.feature.ExtractJson;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;

public class DiffJobSession extends CopyJobSession {
    protected final Boolean autoCorrectMissing;
    protected final Boolean autoCorrectMismatch;
    private final boolean isCounterTable;
    private final boolean forceCounterWhenMissing;
    private final List<String> targetColumnNames;
    private final List<DataType> targetColumnTypes;
    private final List<DataType> originColumnTypes;
    private final int explodeMapKeyIndex;
    private final int explodeMapValueIndex;
    private final List<Integer> constantColumnIndexes;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();
    boolean logTrace = logger.isTraceEnabled();
    private ExtractJson extractJsonFeature;
    private boolean overwriteTarget;

    public DiffJobSession(CqlSession originSession, CqlSession targetSession, PropertyHelper propHelper) {
        super(originSession, targetSession, propHelper);
        autoCorrectMissing = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISSING);
        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

        autoCorrectMismatch = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISMATCH);
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);

        this.isCounterTable = this.originSession.getCqlTable().isCounterTable();
        this.forceCounterWhenMissing = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISSING_COUNTER);
        this.targetColumnNames = this.targetSession.getCqlTable().getColumnNames(false);
        this.targetColumnTypes = this.targetSession.getCqlTable().getColumnCqlTypes();
        this.originColumnTypes = this.originSession.getCqlTable().getColumnCqlTypes();

        ConstantColumns constantColumnsFeature = (ConstantColumns) this.targetSession.getCqlTable()
                .getFeature(Featureset.CONSTANT_COLUMNS);
        if (null != constantColumnsFeature && constantColumnsFeature.isEnabled()) {
            constantColumnIndexes = constantColumnsFeature.getNames().stream().map(targetColumnNames::indexOf)
                    .collect(Collectors.toList());
            if (logDebug)
                logger.debug("Constant Column Indexes {}", this.constantColumnIndexes);
        } else {
            constantColumnIndexes = Collections.emptyList();
        }

        ExplodeMap explodeMapFeature = (ExplodeMap) this.targetSession.getCqlTable().getFeature(Featureset.EXPLODE_MAP);
        if (null != explodeMapFeature && explodeMapFeature.isEnabled()) {
            this.explodeMapKeyIndex = this.targetSession.getCqlTable().indexOf(explodeMapFeature.getKeyColumnName());
            this.explodeMapValueIndex = this.targetSession.getCqlTable()
                    .indexOf(explodeMapFeature.getValueColumnName());
            if (logDebug)
                logger.debug("Explode Map KeyIndex={}, ValueIndex={}", this.explodeMapKeyIndex,
                        this.explodeMapValueIndex);
        } else {
            this.explodeMapKeyIndex = -1;
            this.explodeMapValueIndex = -1;
        }

        extractJsonFeature = (ExtractJson) this.targetSession.getCqlTable().getFeature(Featureset.EXTRACT_JSON);
        overwriteTarget = extractJsonFeature.isEnabled() && extractJsonFeature.overwriteTarget();

        logger.info("CQL -- origin select: {}", this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
        logger.info("CQL -- target select: {}", this.targetSession.getTargetSelectByPKStatement().getCQL());
        logger.info("CQL -- target upsert: {}", this.targetSession.getTargetUpsertStatement().getCQL());
    }

    protected void processPartitionRange(PartitionRange range) {
        BigInteger min = range.getMin(), max = range.getMax();
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        if (null != trackRunFeature)
            trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.STARTED, "");

        AtomicBoolean hasDiff = new AtomicBoolean(false);
        JobCounter jobCounter = range.getJobCounter();
        try {

            PKFactory pkFactory = originSession.getPKFactory();
            OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = originSession
                    .getOriginSelectByPartitionRangeStatement();
            ResultSet resultSet = originSelectByPartitionRangeStatement
                    .execute(originSelectByPartitionRangeStatement.bind(min, max));
            TargetSelectByPKStatement targetSelectByPKStatement = targetSession.getTargetSelectByPKStatement();
            Integer fetchSizeInRows = originSession.getCqlTable().getFetchSizeInRows();

            List<Record> recordsToDiff = new ArrayList<>(fetchSizeInRows);
            StreamSupport.stream(resultSet.spliterator(), false).forEach(originRow -> {
                rateLimiterOrigin.acquire(1);
                Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                jobCounter.increment(JobCounter.CounterType.READ);

                if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                    jobCounter.increment(JobCounter.CounterType.SKIPPED);
                } else {
                    for (Record r : pkFactory.toValidRecordList(record)) {
                        rateLimiterTarget.acquire(1);
                        CompletionStage<AsyncResultSet> targetResult = targetSelectByPKStatement
                                .getAsyncResult(r.getPk());

                        if (null == targetResult) {
                            jobCounter.increment(JobCounter.CounterType.SKIPPED);
                        } else {
                            r.setAsyncTargetRow(targetResult);
                            recordsToDiff.add(r);
                            if (recordsToDiff.size() > fetchSizeInRows) {
                                if (diffAndClear(recordsToDiff, jobCounter)) {
                                    hasDiff.set(true);
                                }
                            }
                        } // targetRecord!=null
                    } // recordSet iterator
                } // shouldFilterRecord
            });
            if (diffAndClear(recordsToDiff, jobCounter)) {
                hasDiff.set(true);
            }

            jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED);
            jobCounter.flush();
            if (hasDiff.get() && null != trackRunFeature) {
                if (jobCounter.getCount(JobCounter.CounterType.MISSING) == jobCounter
                        .getCount(JobCounter.CounterType.CORRECTED_MISSING)
                        && jobCounter.getCount(JobCounter.CounterType.MISMATCH) == jobCounter
                                .getCount(JobCounter.CounterType.CORRECTED_MISMATCH)) {
                    trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.DIFF_CORRECTED,
                            jobCounter.getMetrics());
                } else {
                    trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.DIFF, jobCounter.getMetrics());
                }
            } else if (null != trackRunFeature) {
                trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.PASS, jobCounter.getMetrics());
            }
        } catch (Exception e) {
            jobCounter.increment(JobCounter.CounterType.ERROR,
                    jobCounter.getCount(JobCounter.CounterType.READ) - jobCounter.getCount(JobCounter.CounterType.VALID)
                            - jobCounter.getCount(JobCounter.CounterType.MISSING)
                            - jobCounter.getCount(JobCounter.CounterType.MISMATCH)
                            - jobCounter.getCount(JobCounter.CounterType.SKIPPED));
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max, e);
            logger.error("Error stats " + jobCounter.getMetrics(true));
            jobCounter.flush();
            if (null != trackRunFeature)
                trackRunFeature.updateCdmRun(runId, min, TrackRun.RUN_STATUS.FAIL, jobCounter.getMetrics());
        }
    }

    private boolean diffAndClear(List<Record> recordsToDiff, JobCounter jobCounter) {
        boolean isDiff = recordsToDiff.stream().map(r -> diff(r, jobCounter)).filter(b -> b == true).count() > 0;
        recordsToDiff.clear();
        return isDiff;
    }

    private boolean diff(Record record, JobCounter jobCounter) {
        if (record.getTargetRow() == null) {
            jobCounter.increment(JobCounter.CounterType.MISSING);
            logger.error("Missing target row found for key: {}", record.getPk());
            if (autoCorrectMissing && isCounterTable && !forceCounterWhenMissing) {
                logger.error("{} is true, but not Inserting as {} is not enabled; key : {}",
                        KnownProperties.AUTOCORRECT_MISSING, KnownProperties.AUTOCORRECT_MISSING_COUNTER,
                        record.getPk());
                return true;
            }

            // correct data
            if (autoCorrectMissing) {
                rateLimiterTarget.acquire(1);
                targetSession.getTargetUpsertStatement().putRecord(record);
                jobCounter.increment(JobCounter.CounterType.CORRECTED_MISSING);
                logger.error("Inserted missing row in target: {}", record.getPk());
            }
            return true;
        }

        String diffData = isDifferent(record);
        if (!diffData.isEmpty()) {
            jobCounter.increment(JobCounter.CounterType.MISMATCH);
            logger.error("Mismatch row found for key: {} Mismatch: {}", record.getPk(), diffData);

            if (autoCorrectMismatch) {
                rateLimiterTarget.acquire(1);
                targetSession.getTargetUpsertStatement().putRecord(record);
                jobCounter.increment(JobCounter.CounterType.CORRECTED_MISMATCH);
                logger.error("Corrected mismatch row in target: {}", record.getPk());
            }

            return true;
        } else {
            jobCounter.increment(JobCounter.CounterType.VALID);
            return false;
        }
    }

    private String isDifferent(Record record) {
        EnhancedPK pk = record.getPk();
        Row originRow = record.getOriginRow();
        Row targetRow = record.getTargetRow();

        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, targetColumnNames.size()).parallel().forEach(targetIndex -> {
            String previousLabel = ThreadContext.get(THREAD_CONTEXT_LABEL);
            try {
                ThreadContext.put(THREAD_CONTEXT_LABEL, pk + ":" + targetColumnNames.get(targetIndex));
                Object origin = null;
                int originIndex = -2; // this to distinguish default from indexOf result
                Object targetAsOriginType = null;
                try {
                    if (constantColumnIndexes.contains(targetIndex)) {
                        if (logTrace)
                            logger.trace("PK {}, targetIndex {} skipping constant column {}", pk, targetIndex,
                                    targetColumnNames.get(targetIndex));
                        return; // nothing to compare in origin
                    }

                    targetAsOriginType = targetSession.getCqlTable().getAndConvertData(targetIndex, targetRow);
                    if (targetIndex == extractJsonFeature.getTargetColumnIndex()) {
                        if (!overwriteTarget && null != targetAsOriginType) {
                            return; // skip validation when target has data
                        }
                        originIndex = extractJsonFeature.getOriginColumnIndex();
                        origin = extractJsonFeature.extract(originRow.getString(originIndex));
                    } else {
                        originIndex = targetSession.getCqlTable().getCorrespondingIndex(targetIndex);
                        if (originIndex >= 0) {
                            origin = originSession.getCqlTable().getData(originIndex, originRow);
                            if (logTrace)
                                logger.trace(
                                        "PK {}, targetIndex {} column {} using value from origin table at index {}: {}",
                                        pk, targetIndex, targetColumnNames.get(targetIndex), originIndex, origin);
                        } else if (targetIndex == explodeMapKeyIndex) {
                            origin = pk.getExplodeMapKey();
                            if (logTrace)
                                logger.trace("PK {}, targetIndex {} column {} using explodeMapKey stored on PK: {}", pk,
                                        targetIndex, targetColumnNames.get(targetIndex), origin);
                        } else if (targetIndex == explodeMapValueIndex) {
                            origin = pk.getExplodeMapValue();
                            if (logTrace)
                                logger.trace("PK {}, targetIndex {} column {} using explodeMapValue stored on PK: {}",
                                        pk, targetIndex, targetColumnNames.get(targetIndex), origin);
                        } else if (targetIndex == extractJsonFeature.getTargetColumnIndex()) {
                            originIndex = extractJsonFeature.getOriginColumnIndex();
                        }
                    }

                    if (logDebug)
                        logger.debug(
                                "Diff PK {}, target/origin index: {}/{} target/origin column: {}/{} target/origin value: {}/{}",
                                pk, targetIndex, originIndex, targetColumnNames.get(targetIndex),
                                originIndex < 0 ? "null"
                                        : originSession.getCqlTable().getColumnNames(false).get(originIndex),
                                targetAsOriginType, origin);
                    if (null != origin && DataUtility.diff(origin, targetAsOriginType)) {
                        String originContent = CqlData
                                .getFormattedContent(CqlData.toType(originColumnTypes.get(originIndex)), origin);
                        String targetContent = CqlData.getFormattedContent(
                                CqlData.toType(targetColumnTypes.get(targetIndex)), targetAsOriginType);
                        diffData.append("Target column:").append(targetColumnNames.get(targetIndex)).append("-origin[")
                                .append(originContent).append("]").append("-target[").append(targetContent)
                                .append("]; ");
                    } else if (null == origin && null != targetAsOriginType) {
                        diffData.append("Target column:").append(targetColumnNames.get(targetIndex))
                                .append(" origin is null, target is ").append(targetAsOriginType).append("; ");
                    }
                } catch (Exception e) {
                    String exceptionName;
                    String myClassMethodLine = DataUtility.getMyClassMethodLine(e);
                    if (e instanceof ArrayIndexOutOfBoundsException) {
                        exceptionName = "ArrayIndexOutOfBoundsException@" + myClassMethodLine;
                    } else {
                        exceptionName = e + "@" + myClassMethodLine;
                    }
                    diffData.append("Target column:").append(targetColumnNames.get(targetIndex)).append(" Exception ")
                            .append(exceptionName).append(" targetIndex:").append(targetIndex).append(" originIndex:")
                            .append(originIndex).append("; ");
                }
            } finally {
                ThreadContext.put(THREAD_CONTEXT_LABEL, previousLabel);
            }
        });
        return diffData.toString();
    }

}
