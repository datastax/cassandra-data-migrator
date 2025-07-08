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

import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class GuardrailCheckJobSession extends AbstractJobSession<PartitionRange> {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected GuardrailCheckJobSession(CqlSession originSession, CqlSession targetSession, PropertyHelper propHelper) {
        super(originSession, targetSession, propHelper);
        if (!guardrailFeature.isEnabled()) {
            logger.error("GuardrailCheckJobSession is disabled - is it configured correctly?");
            return;
        }

        logger.info("CQL -- origin select: {}", this.originSession.getOriginSelectByPartitionRangeStatement().getCQL());
    }

    protected void processPartitionRange(PartitionRange range) {
        BigInteger min = range.getMin(), max = range.getMax();
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel(min, max));
        JobCounter jobCounter = range.getJobCounter();
        try {
            logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
            OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = this.originSession
                    .getOriginSelectByPartitionRangeStatement();
            ResultSet resultSet = originSelectByPartitionRangeStatement
                    .execute(originSelectByPartitionRangeStatement.bind(min, max));
            String checkString;
            for (Row originRow : resultSet) {
                rateLimiterOrigin.acquire(1);
                jobCounter.increment(JobCounter.CounterType.READ);

                checkString = guardrailFeature.guardrailChecks(originRow);
                if (checkString != null && !checkString.isEmpty()) {
                    jobCounter.increment(JobCounter.CounterType.LARGE);
                    logger.error("Guardrails failed for row {}", checkString);
                } else {
                    jobCounter.increment(JobCounter.CounterType.VALID);
                }
            }
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED);
        } catch (Exception e) {
            jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED);
            logger.error("Error occurred ", e);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max);
        } finally {
            jobCounter.flush();
        }

        ThreadContext.remove(THREAD_CONTEXT_LABEL);
    }
}
