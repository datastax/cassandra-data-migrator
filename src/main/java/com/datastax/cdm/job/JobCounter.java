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

import java.io.Serializable;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.job.IJobSessionFactory.JobType;

public class JobCounter implements Serializable {

    private static final long serialVersionUID = 7016816604237020549L;

    public enum CounterType {
        READ, WRITE, VALID, ERROR, MISMATCH, MISSING, CORRECTED_MISSING, CORRECTED_MISMATCH, SKIPPED, UNFLUSHED, LARGE
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final HashMap<CounterType, CounterUnit> counterMap = new HashMap<>();

    public JobCounter(JobType jobType) {
        switch (jobType) {
        case MIGRATE:
            setRegisteredTypes(CounterType.READ, CounterType.WRITE, CounterType.SKIPPED, CounterType.ERROR,
                    CounterType.UNFLUSHED);
            break;
        case VALIDATE:
            setRegisteredTypes(CounterType.READ, CounterType.VALID, CounterType.MISMATCH,
                    CounterType.CORRECTED_MISMATCH, CounterType.MISSING, CounterType.CORRECTED_MISSING,
                    CounterType.SKIPPED, CounterType.ERROR);
            break;
        case GUARDRAIL:
            setRegisteredTypes(CounterType.READ, CounterType.VALID, CounterType.SKIPPED, CounterType.LARGE);
            break;
        default:
            throw new IllegalArgumentException("JobType " + jobType + " is not registered");
        }
    }

    private void setRegisteredTypes(CounterType... registeredTypes) {
        counterMap.clear();
        for (CounterType type : registeredTypes) {
            counterMap.put(type, new CounterUnit());
        }
    }

    private CounterUnit getCounterUnit(CounterType counterType) {
        if (!counterMap.containsKey(counterType)) {
            throw new IllegalArgumentException("CounterType " + counterType + " is not registered");
        }
        return (counterMap.get(counterType));
    }

    public long getCount(CounterType counterType, boolean global) {
        return global ? getCounterUnit(counterType).getGlobalCounter() : getCounterUnit(counterType).getThreadCounter();
    }

    public long getCount(CounterType counterType) {
        return getCount(counterType, false);
    }

    // Method to reset thread-specific counters for given type
    public void threadReset(CounterType counterType) {
        getCounterUnit(counterType).resetThreadCounter();
    }

    // Method to increment thread-specific counters by a given value
    public void threadIncrement(CounterType counterType, long incrementBy) {
        getCounterUnit(counterType).incrementThreadCounter(incrementBy);
    }

    // Method to increment thread-specific counters by 1
    public void threadIncrement(CounterType counterType) {
        threadIncrement(counterType, 1);
    }

    // Method to increment global counters based on thread-specific counters
    public void globalIncrement() {
        for (CounterType type : counterMap.keySet()) {
            getCounterUnit(type).addThreadToGlobalCounter();
        }
    }

    // Method to get current counts (both thread-specific and global) as a formatted
    // string
    public String getThreadCounters(boolean global) {
        StringBuilder sb = new StringBuilder();
        for (CounterType type : counterMap.keySet()) {
            long value = global ? getCounterUnit(type).getGlobalCounter() : getCounterUnit(type).getThreadCounter();
            sb.append(type.name()).append(": ").append(value).append("; ");
        }
        // Remove the trailing comma and space
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
        }
        return sb.toString();
    }

    public void add(JobCounter v) {
        for (CounterType type : counterMap.keySet()) {
            getCounterUnit(type).setGlobalCounter(getCounterUnit(type).getGlobalCounter() + v.getCount(type, true));
        }
    }

    public void reset() {
        for (CounterType type : counterMap.keySet()) {
            getCounterUnit(type).setGlobalCounter(0);
        }
    }

    public boolean isZero() {
        for (CounterType type : counterMap.keySet()) {
            if (getCounterUnit(type).getGlobalCounter() > 0) {
                return false;
            }
        }
        return true;
    }

    public void printMetrics(long runId, TrackRun trackRunFeature) {
        if (null != trackRunFeature) {
            trackRunFeature.endCdmRun(runId, getThreadCounters(true));
        }
        logger.info("################################################################################################");
        if (counterMap.containsKey(CounterType.READ))
            logger.info("Final Read Record Count: {}", counterMap.get(CounterType.READ).getGlobalCounter());
        if (counterMap.containsKey(CounterType.MISMATCH))
            logger.info("Final Mismatch Record Count: {}", counterMap.get(CounterType.MISMATCH).getGlobalCounter());
        if (counterMap.containsKey(CounterType.CORRECTED_MISMATCH))
            logger.info("Final Corrected Mismatch Record Count: {}",
                    counterMap.get(CounterType.CORRECTED_MISMATCH).getGlobalCounter());
        if (counterMap.containsKey(CounterType.MISSING))
            logger.info("Final Missing Record Count: {}", counterMap.get(CounterType.MISSING).getGlobalCounter());
        if (counterMap.containsKey(CounterType.CORRECTED_MISSING))
            logger.info("Final Corrected Missing Record Count: {}",
                    counterMap.get(CounterType.CORRECTED_MISSING).getGlobalCounter());
        if (counterMap.containsKey(CounterType.VALID))
            logger.info("Final Valid Record Count: {}", counterMap.get(CounterType.VALID).getGlobalCounter());
        if (counterMap.containsKey(CounterType.SKIPPED))
            logger.info("Final Skipped Record Count: {}", counterMap.get(CounterType.SKIPPED).getGlobalCounter());
        if (counterMap.containsKey(CounterType.WRITE))
            logger.info("Final Write Record Count: {}", counterMap.get(CounterType.WRITE).getGlobalCounter());
        if (counterMap.containsKey(CounterType.ERROR))
            logger.info("Final Error Record Count: {}", counterMap.get(CounterType.ERROR).getGlobalCounter());
        if (counterMap.containsKey(CounterType.LARGE))
            logger.info("Final Large Record Count: {}", counterMap.get(CounterType.LARGE).getGlobalCounter());
        logger.info("################################################################################################");
    }

}
