package com.datastax.cdm.job;

import com.datastax.cdm.feature.Guardrail;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.cql.statement.OriginSelectByPKStatement;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CopyPKJobSession extends AbstractJobSession<SplitPartitions.PKRows> {

    private static CopyPKJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong missingCounter = new AtomicLong(0);
    protected AtomicLong skipCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    private final PKFactory pkFactory;
    private final List<Class> originPKClasses;
    private final boolean isCounterTable;
    private OriginSelectByPKStatement originSelectByPKStatement;

    protected CopyPKJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc, true);
        pkFactory = this.originSession.getPKFactory();
        isCounterTable = this.originSession.getCqlTable().isCounterTable();
        originPKClasses = this.originSession.getCqlTable().getPKClasses();

        logger.info("CQL -- origin select: {}",this.originSession.getOriginSelectByPKStatement().getCQL());
    }

    @Override
    public void processSlice(SplitPartitions.PKRows slice) {
        this.getRowAndInsert(slice);
    }

    public void getRowAndInsert(SplitPartitions.PKRows rowsList) {
        originSelectByPKStatement = originSession.getOriginSelectByPKStatement();
        for (String row : rowsList.pkRows) {
            readCounter.incrementAndGet();
            EnhancedPK pk = toEnhancedPK(row);
            if (null == pk || pk.isError()) {
                missingCounter.incrementAndGet();
                logger.error("Could not build PK object with value <{}>; error is: {}", row, (null == pk ? "null" : pk.getMessages()));
                return;
            }

            Record recordFromOrigin = originSelectByPKStatement.getRecord(pk);
            if (null == recordFromOrigin) {
                missingCounter.incrementAndGet();
                logger.error("Could not find origin row with primary-key: {}", row);
                return;
            }
            Row originRow = recordFromOrigin.getOriginRow();

            Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
            if (originSelectByPKStatement.shouldFilterRecord(record)) {
                skipCounter.incrementAndGet();
                return;
            }

            if (guardrailEnabled) {
                String guardrailCheck = guardrailFeature.guardrailChecks(record);
                if (guardrailCheck != null && guardrailCheck != Guardrail.CLEAN_CHECK) {
                    logger.error("Guardrails failed for PrimaryKey {}; {}", record.getPk(), guardrailCheck);
                    skipCounter.incrementAndGet();
                    return;
                }
            }

            writeLimiter.acquire(1);
            targetSession.getTargetUpsertStatement().putRecord(record);
            writeCounter.incrementAndGet();

            if (readCounter.get() % printStatsAfter == 0) {
                printCounts(false);
            }
        }

        printCounts(true);
    }

    @Override
    public void printCounts(boolean isFinal) {
        if (isFinal) {
            logger.info("################################################################################################");
        }
        logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
        logger.info("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
        logger.info("ThreadID: {} Skipped Record Count: {}", Thread.currentThread().getId(), skipCounter.get());
        logger.info("ThreadID: {} Inserted Record Count: {}", Thread.currentThread().getId(), writeCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private EnhancedPK toEnhancedPK(String rowString) {
        String[] pkFields = rowString.split(" %% ");
        List<Object> values = new ArrayList<>(originPKClasses.size());
        if (logger.isDebugEnabled()) logger.debug("rowString={}, pkFields={}", rowString, pkFields);
        for (int i=0; i<pkFields.length; i++) {
            PropertyEditor editor = PropertyEditorManager.findEditor(originPKClasses.get(i));
            editor.setAsText(pkFields[i]);
            values.add(editor.getValue());
        }
        return pkFactory.toEnhancedPK(values, pkFactory.getPKClasses(PKFactory.Side.ORIGIN));
    }

}