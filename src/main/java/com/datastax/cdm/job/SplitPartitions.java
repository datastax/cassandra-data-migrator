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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitPartitions {

    public static Logger logger = LoggerFactory.getLogger(SplitPartitions.class.getName());

    public static List<Partition> getRandomSubPartitions(int numSplits, BigInteger min, BigInteger max,
            int coveragePercent) {
        logger.info("ThreadID: {} Splitting min: {} max: {}", Thread.currentThread().getId(), min, max);
        List<Partition> partitions = getSubPartitions(numSplits, min, max, coveragePercent);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        return partitions;
    }

    private static List<Partition> getSubPartitions(int numSplits, BigInteger min, BigInteger max,
            int coveragePercent) {
        if (coveragePercent < 1 || coveragePercent > 100) {
            coveragePercent = 100;
        }
        BigInteger curMax = new BigInteger(min.toString());
        BigInteger partitionSize = max.subtract(min).divide(BigInteger.valueOf(numSplits));
        List<Partition> partitions = new ArrayList<Partition>();
        if (partitionSize.compareTo(new BigInteger("0")) == 0) {
            partitionSize = new BigInteger("100000");
        }
        boolean exausted = false;
        while (curMax.compareTo(max) <= 0) {
            BigInteger curMin = new BigInteger(curMax.toString());
            BigInteger newCurMax = curMin.add(partitionSize);
            if (newCurMax.compareTo(curMax) == -1) {
                newCurMax = new BigInteger(max.toString());
                exausted = true;
            }
            if (newCurMax.compareTo(max) == 1) {
                newCurMax = new BigInteger(max.toString());
                exausted = true;
            }
            curMax = newCurMax;

            BigInteger range = curMax.subtract(curMin);
            BigInteger curRange = range.multiply(BigInteger.valueOf(coveragePercent)).divide(BigInteger.valueOf(100));
            partitions.add(new Partition(curMin, curMin.add(curRange)));
            if (exausted) {
                break;
            }
            curMax = curMax.add(BigInteger.ONE);
        }

        return partitions;
    }

    public static class Partition implements Serializable {
        private static final long serialVersionUID = 1L;

        private final BigInteger min;
        private final BigInteger max;

        public Partition(BigInteger min, BigInteger max) {
            this.min = min;
            this.max = max;
        }

        public BigInteger getMin() {
            return min;
        }

        public BigInteger getMax() {
            return max;
        }

        public String toString() {
            return "Processing partition for token range " + min + " to " + max;
        }
    }
}