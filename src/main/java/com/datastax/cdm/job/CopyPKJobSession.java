package com.datastax.cdm.job;

import com.datastax.cdm.cql.statement.OriginSelectByPKStatement;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.Guardrail;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.ArrayList;
import java.util.List;

public class CopyPKJobSession extends AbstractJobSession<SplitPartitions.PKRows> {

    private final PKFactory pkFactory;
    private final List<Class> originPKClasses;
    private final boolean isCounterTable;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private OriginSelectByPKStatement originSelectByPKStatement;

    protected CopyPKJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc, true);
        this.jobCounter.setRegisteredTypes(JobCounter.CounterType.READ, JobCounter.CounterType.WRITE, JobCounter.CounterType.SKIPPED, JobCounter.CounterType.MISSING);
        pkFactory = this.originSession.getPKFactory();
        isCounterTable = this.originSession.getCqlTable().isCounterTable();
        originPKClasses = this.originSession.getCqlTable().getPKClasses();

        logger.info("CQL -- origin select: {}", this.originSession.getOriginSelectByPKStatement().getCQL());
    }

    @Override
    public void processSlice(SplitPartitions.PKRows slice) {
        this.getRowAndInsert(slice);
    }

    public void getRowAndInsert(SplitPartitions.PKRows rowsList) {
        originSelectByPKStatement = originSession.getOriginSelectByPKStatement();
        for (String row : rowsList.getPkRows()) {
            jobCounter.threadIncrement(JobCounter.CounterType.READ);
            EnhancedPK pk = toEnhancedPK(row);
            if (null == pk || pk.isError()) {
                jobCounter.threadIncrement(JobCounter.CounterType.MISSING);
                logger.error("Could not build PK object with value <{}>; error is: {}", row, (null == pk ? "null" : pk.getMessages()));
                return;
            }

            rateLimiterOrigin.acquire(1);
            Record recordFromOrigin = originSelectByPKStatement.getRecord(pk);
            if (null == recordFromOrigin) {
                jobCounter.threadIncrement(JobCounter.CounterType.MISSING);
                logger.error("Could not find origin row with primary-key: {}", row);
                return;
            }
            Row originRow = recordFromOrigin.getOriginRow();

            Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
            if (originSelectByPKStatement.shouldFilterRecord(record)) {
                jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED);
                return;
            }

            if (guardrailEnabled) {
                String guardrailCheck = guardrailFeature.guardrailChecks(record);
                if (guardrailCheck != null && guardrailCheck != Guardrail.CLEAN_CHECK) {
                    logger.error("Guardrails failed for PrimaryKey {}; {}", record.getPk(), guardrailCheck);
                    jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED);
                    return;
                }
            }

            rateLimiterTarget.acquire(1);
            targetSession.getTargetUpsertStatement().putRecord(record);
            jobCounter.threadIncrement(JobCounter.CounterType.WRITE);

            jobCounter.globalIncrement();
            printCounts(false);
        }

        printCounts(true);
    }

    private EnhancedPK toEnhancedPK(String rowString) {
        String[] pkFields = rowString.split(" %% ");
        List<Object> values = new ArrayList<>(originPKClasses.size());
        if (logger.isDebugEnabled()) logger.debug("rowString={}, pkFields={}", rowString, pkFields);
        for (int i = 0; i < pkFields.length; i++) {
            PropertyEditor editor = PropertyEditorManager.findEditor(originPKClasses.get(i));
            editor.setAsText(pkFields[i]);
            values.add(editor.getValue());
        }
        return pkFactory.toEnhancedPK(values, pkFactory.getPKClasses(PKFactory.Side.ORIGIN));
    }

}