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

import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SplitPartitions {

    public static Logger logger = LoggerFactory.getLogger(SplitPartitions.class.getName());
    private static final int MAX_NUM_PARTS_FOR_PARTITION_FILE = 10;

    public static List<Partition> getRandomSubPartitions(int numSplits, BigInteger min, BigInteger max, int coveragePercent) {
        logger.info("ThreadID: {} Splitting min: {} max: {}", Thread.currentThread().getId(), min, max);
        List<Partition> partitions = getSubPartitions(numSplits, min, max, coveragePercent);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        return partitions;
    }

    public static List<Partition> getSubPartitionsFromFile(int numSplits, String inputFilename) throws IOException {
        logger.info("ThreadID: {} Splitting partitions in file: {} using a split-size of {}"
                , Thread.currentThread().getId(), inputFilename, numSplits);
        if (numSplits > 10) {
            logger.warn("Resetting spark.cdm.perfops.numParts value of {} to max allowed value of {} when using a partition file: {}",
                    numSplits, MAX_NUM_PARTS_FOR_PARTITION_FILE, inputFilename);
            numSplits = MAX_NUM_PARTS_FOR_PARTITION_FILE;
        }
        List<Partition> partitions = new ArrayList<Partition>();
        BufferedReader reader = getfileReader(inputFilename);
        String line = null;
        PartitionMinMax pMinMax;
        while ((line = reader.readLine()) != null) {
            try {
                pMinMax = new PartitionMinMax(line);
                if (pMinMax.hasError) {
                    logger.error("Skipping " + pMinMax.error);
                    continue;
                }
                partitions.addAll(getSubPartitions(numSplits, pMinMax.min, pMinMax.max, 100));
            } catch (Exception e) {
                logger.error("Skipping partition: {}", line, e);
            }
        }

        return partitions;
    }

    static class PartitionMinMax {
        static final Pattern pat = Pattern.compile("^-?[0-9]*,-?[0-9]*");
        public BigInteger min;
        public BigInteger max;
        public boolean hasError = false;
        public String error;

        public PartitionMinMax(String line) {
            line = line.replaceAll(" ", "");
            if (!pat.matcher(line).matches()) {
                error = "Invaliding partition line: " + line;
                hasError = true;
                return;
            }
            String[] minMax = line.split(",");
            min = new BigInteger(minMax[0]);
            max = new BigInteger(minMax[1]);
        }
    }

    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    private static List<Partition> getSubPartitions(int numSplits, BigInteger min, BigInteger max, int coveragePercent) {
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

    private static BufferedReader getfileReader(String fileName) {
        try {
            return new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException("No '" + fileName + "' file found!! Add this file in the current folder & rerun!");
        }
    }

    public static boolean appendPartitionOnDiff(PropertyHelper propertyHelper) {
        return Boolean.TRUE.equals(propertyHelper.getBoolean(KnownProperties.TOKEN_RANGE_PARTITION_FILE_APPEND_ON_DIFF));
    }

    public static String getPartitionFileInput(PropertyHelper propertyHelper) {
        if (!StringUtils.isAllBlank(propertyHelper.getString(KnownProperties.TOKEN_RANGE_PARTITION_FILE_INPUT))) {
            return propertyHelper.getString(KnownProperties.TOKEN_RANGE_PARTITION_FILE_INPUT);
        }

        return "./" + propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) + "_partitions.csv";
    }

    public static String getPartitionFileOutput(PropertyHelper propertyHelper) {
        if (!StringUtils.isAllBlank(propertyHelper.getString(KnownProperties.TOKEN_RANGE_PARTITION_FILE_OUTPUT))) {
            return propertyHelper.getString(KnownProperties.TOKEN_RANGE_PARTITION_FILE_OUTPUT);
        }

        return "./" + propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) + "_partitions.csv";
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