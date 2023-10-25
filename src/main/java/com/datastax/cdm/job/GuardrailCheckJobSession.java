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

public class GuardrailCheckJobSession extends AbstractJobSession<SplitPartitions.Partition> {

    private static GuardrailCheckJobSession guardrailJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final PKFactory pkFactory;

    protected GuardrailCheckJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);
        this.jobCounter.setRegisteredTypes(JobCounter.CounterType.READ, JobCounter.CounterType.VALID, JobCounter.CounterType.SKIPPED, JobCounter.CounterType.LARGE);

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
                rateLimiterOrigin.acquire(1);
                jobCounter.threadIncrement(JobCounter.CounterType.READ);

                Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                    jobCounter.threadIncrement(JobCounter.CounterType.SKIPPED);
                    continue;
                }

                for (Record r : pkFactory.toValidRecordList(record)) {
                    checkString = guardrailFeature.guardrailChecks(r);
                    if (checkString != null && !checkString.isEmpty()) {
                        jobCounter.threadIncrement(JobCounter.CounterType.LARGE);
                        logger.error("Guardrails failed for PrimaryKey {}; {}", r.getPk(), checkString);
                    }
                    else {
                        jobCounter.threadIncrement(JobCounter.CounterType.VALID);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error occurred ", e);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max);
        } finally {
            jobCounter.globalIncrement();
            printCounts(false);
        }

        ThreadContext.remove(THREAD_CONTEXT_LABEL);
    }
}
