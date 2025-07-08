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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.job.IJobSessionFactory.JobType;

public class JobCounter implements Serializable {

    private static final long serialVersionUID = 7016816604237020549L;

    public enum CounterType {
        READ, WRITE, MISMATCH, CORRECTED_MISMATCH, MISSING, CORRECTED_MISSING, VALID, SKIPPED, LARGE, ERROR, UNFLUSHED,
        PARTITIONS_PASSED, PARTITIONS_FAILED
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final HashMap<CounterType, CounterUnit> counterMap = new HashMap<>();

    public JobCounter(JobType jobType) {
        switch (jobType) {
        case MIGRATE:
            setRegisteredTypes(CounterType.READ, CounterType.WRITE, CounterType.SKIPPED, CounterType.ERROR,
                    CounterType.UNFLUSHED, CounterType.PARTITIONS_PASSED, CounterType.PARTITIONS_FAILED);
            break;
        case VALIDATE:
            setRegisteredTypes(CounterType.READ, CounterType.VALID, CounterType.MISMATCH,
                    CounterType.CORRECTED_MISMATCH, CounterType.MISSING, CounterType.CORRECTED_MISSING,
                    CounterType.SKIPPED, CounterType.ERROR, CounterType.PARTITIONS_PASSED,
                    CounterType.PARTITIONS_FAILED);
            break;
        case GUARDRAIL:
            setRegisteredTypes(CounterType.READ, CounterType.VALID, CounterType.SKIPPED, CounterType.LARGE,
                    CounterType.PARTITIONS_PASSED, CounterType.PARTITIONS_FAILED);
            break;
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

    public long getCount(CounterType type, boolean interim) {
        return interim ? getCounterUnit(type).getInterimCount() : getCounterUnit(type).getCount();
    }

    public long getCount(CounterType counterType) {
        return getCount(counterType, false);
    }

    public void reset(CounterType type) {
        getCounterUnit(type).reset();
    }

    public void increment(CounterType counterType, long incrementBy) {
        getCounterUnit(counterType).increment(incrementBy);
    }

    public void increment(CounterType counterType) {
        increment(counterType, 1);
    }

    public void flush() {
        for (CounterType type : counterMap.keySet()) {
            getCounterUnit(type).addToCount();
        }
    }

    public String getMetrics() {
        return getMetrics(false);
    }

    public String getMetrics(boolean interim) {
        StringBuilder sb = new StringBuilder();
        for (CounterType type : CounterType.values()) {
            if (counterMap.containsKey(type)) {
                if (!interim && type == CounterType.UNFLUSHED) {
                    continue;
                }
                sb.append(printFriendlyCase(type.name())).append(": ").append(getCount(type, interim)).append("; ");
            }
        }
        // Remove the trailing comma and space
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
        }
        return sb.toString();
    }

    // Capitalizes the first letter of each word in a sentence
    private String printFriendlyCase(String sentence) {
        String[] words = sentence.toLowerCase().split("_");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            sb.append(StringUtils.capitalize(word)).append(" ");
        }
        return sb.toString().trim();
    }

    public void add(JobCounter v) {
        for (CounterType type : counterMap.keySet()) {
            getCounterUnit(type).setCount(getCounterUnit(type).getCount() + v.getCount(type));
        }
    }

    public void reset() {
        for (CounterType type : counterMap.keySet()) {
            reset(type);
        }
    }

    public boolean isZero() {
        for (CounterType type : counterMap.keySet()) {
            if (getCounterUnit(type).getCount() > 0 || getCounterUnit(type).getInterimCount() > 0) {
                return false;
            }
        }
        return true;
    }

    public void printMetrics(long runId, TrackRun trackRunFeature) {
        logger.info("################################################################################################");
        if (null != trackRunFeature) {
            trackRunFeature.endCdmRun(runId, getMetrics());
            logger.info("RunId: {}", runId);
        }
        for (CounterType type : CounterType.values()) {
            if (counterMap.containsKey(type)) {
                if (type == CounterType.UNFLUSHED) {
                    continue;
                }
                if (type == CounterType.PARTITIONS_PASSED || type == CounterType.PARTITIONS_FAILED) {
                    logger.info("Final " + printFriendlyCase(type.name()) + ": {}", counterMap.get(type).getCount());
                } else {
                    logger.info("Final " + printFriendlyCase(type.name()) + " Record Count: {}",
                            counterMap.get(type).getCount());
                }
            }
        }
        logger.info("################################################################################################");
    }

}
