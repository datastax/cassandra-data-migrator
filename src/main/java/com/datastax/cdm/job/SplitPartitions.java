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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SplitPartitions {

    public static Logger logger = LoggerFactory.getLogger(SplitPartitions.class.getName());

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
        List<Partition> partitions = new ArrayList<Partition>();
        BufferedReader reader = getfileReader(inputFilename);
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] minMax = line.split(",");
            try {
                partitions.addAll(getSubPartitions(numSplits, new BigInteger(minMax[0]), new BigInteger(minMax[1]), 100));
            } catch (Exception e) {
                logger.error("Skipping partition: {}", line, e);
            }
        }

        return partitions;
    }

    public static List<PKRows> getRowPartsFromFile(int numSplits, String inputFilename) throws IOException {
        logger.info("ThreadID: {} Splitting rows in file: {} using a split-size of {}"
                , Thread.currentThread().getId(), inputFilename, numSplits);
        List<String> pkRows = new ArrayList<String>();
        BufferedReader reader = getfileReader(inputFilename);
        String pkRow = null;
        while ((pkRow = reader.readLine()) != null) {
            if (pkRow.startsWith("#")) {
                continue;
            }
            pkRows.add(pkRow);
        }
        int partSize = pkRows.size() / numSplits;
        if (partSize == 0) {
            partSize = pkRows.size();
        }
        return batches(pkRows, partSize).map(l -> (new PKRows(l))).collect(Collectors.toList());
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

    public static String getPartitionFile(PropertyHelper propertyHelper) {
        String filePath = propertyHelper.getString(KnownProperties.TOKEN_RANGE_PARTITION_FILE);
        if (StringUtils.isAllBlank(filePath)) {
            filePath = "./" + propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) + "_partitions.csv";
        }

        return filePath;
    }

    public static class PKRows implements Serializable {
        private static final long serialVersionUID = 1L;
        private List<String> pkRows;

        public List<String> getPkRows() {
            return pkRows;
        }

        public PKRows(List<String> rows) {
            pkRows = new ArrayList<>(rows);
        }
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