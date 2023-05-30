package com.datastax.cdm.job;

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GuardrailCheckJobSession extends AbstractJobSession<SplitPartitions.Partition> {

    private static GuardrailCheckJobSession guardrailJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong validCounter = new AtomicLong(0);
    protected AtomicLong skippedCounter = new AtomicLong(0);
    protected AtomicLong largeRowCounter = new AtomicLong(0);

    private final PKFactory pkFactory;

    protected GuardrailCheckJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);

        pkFactory = this.originSession.getPKFactory();

        if (!guardrailFeature.isEnabled()) {
            logger.error("GuardrailCheckJobSession is disabled - is it configured correctly?");
            return;
        }

        logger.info("CQL -- origin select: {}",this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
    }

    @Override
    public void processSlice(SplitPartitions.Partition slice) {
        this.guardrailCheck(slice.getMin(), slice.getMax());
    }

    public void guardrailCheck(BigInteger min, BigInteger max) {
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min,max));
        try {
            logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
            OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = this.originSession.getOriginSelectByPartitionRangeStatement();
            ResultSet resultSet = originSelectByPartitionRangeStatement.execute(originSelectByPartitionRangeStatement.bind(min, max));
            String checkString;
            for (Row originRow : resultSet) {
                originLimiter.acquire(1);
                readCounter.addAndGet(1);

                if (readCounter.get() % printStatsAfter == 0) {
                    printCounts(false);
                }

                Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                    skippedCounter.addAndGet(1);
                    continue;
                }

                for (Record r : pkFactory.toValidRecordList(record)) {
                    checkString = guardrailFeature.guardrailChecks(r);
                    if (checkString != null && !checkString.isEmpty()) {
                        largeRowCounter.addAndGet(1);
                        logger.error("Guardrails failed for PrimaryKey {}; {}", r.getPk(), checkString);
                    }
                    else {
                        validCounter.addAndGet(1);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error occurred ", e);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max);
        }

        ThreadContext.remove(THREAD_CONTEXT_LABEL);
    }

    @Override
    public synchronized void printCounts(boolean isFinal) {
        String msg = "ThreadID: " + Thread.currentThread().getId();
        if (isFinal) {
            msg += " Final";
            logger.info("################################################################################################");
        }
        logger.info("{} Read Record Count: {}", msg, readCounter.get());
        logger.info("{} Valid Record Count: {}", msg, validCounter.get());
        logger.info("{} Large Record Count: {}", msg, largeRowCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }
}
