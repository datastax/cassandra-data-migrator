package datastax.astra.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SplitPartitions {

    public final static Long MIN_PARTITION = Long.MIN_VALUE;
    public final static Long MAX_PARTITION = Long.MAX_VALUE;
    public static Logger logger = LoggerFactory.getLogger(SplitPartitions.class.getName());

    public static void main(String[] args) throws IOException {
        Collection<Partition> partitions = getSubPartitions(2, BigInteger.valueOf(1),
                BigInteger.valueOf(1000), 100);
//        Collection<Partition> partitions = getSubPartitionsFromFile(3);
        for (Partition partition : partitions) {
            System.out.println(partition);
        }
    }

    public static Collection<Partition> getRandomSubPartitions(int splitSize, BigInteger min, BigInteger max, int coveragePercent) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Splitting min: " + min + " max:" + max);
        List<Partition> partitions = getSubPartitions(splitSize, min, max, coveragePercent);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        return partitions;
    }

    public static List<Partition> getSubPartitionsFromFile(int splitSize) throws IOException {
        logger.info("TreadID: " + Thread.currentThread().getId() +
                " Splitting partitions in file: ./partitions.csv using a split-size of " + splitSize);
        List<Partition> partitions = new ArrayList<Partition>();
        BufferedReader reader = Util.getfileReader("./partitions.csv");
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            String[] minMax = line.split(",");
            try {
                partitions.addAll(getSubPartitions(splitSize, new BigInteger(minMax[0]), new BigInteger(minMax[1]), 100));
            } catch (Exception e) {
                logger.error("Skipping partition: " + line, e);
            }
        }

        return partitions;
    }

    public static List<PKRows> getRowPartsFromFile(int splitSize) throws IOException {
        logger.info("TreadID: " + Thread.currentThread().getId() +
                " Splitting rows in file: ./primary_key_rows.csv using a split-size of " + splitSize);
        List<String> pkRows = new ArrayList<String>();
        BufferedReader reader = Util.getfileReader("./primary_key_rows.csv");
        String pkRow = null;
        while ((pkRow = reader.readLine()) != null) {
            if (pkRow.startsWith("#")) {
                continue;
            }
            pkRows.add(pkRow);
        }
        int partSize = pkRows.size() / splitSize;
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

    private static List<Partition> getSubPartitions(int splitSize, BigInteger min, BigInteger max, int coveragePercent) {
        if (coveragePercent < 1 || coveragePercent > 100) {
            coveragePercent = 100;
        }
        BigInteger curMax = new BigInteger(min.toString());
        BigInteger partitionSize = max.subtract(min).divide(BigInteger.valueOf(splitSize));
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

    public static class PKRows implements Serializable {
        List<String> pkRows;

        public PKRows(List<String> rows) {
            pkRows = rows;
        }
    }

    public static class Partition implements Serializable {
        private static final long serialVersionUID = 1L;

        private BigInteger min;
        private BigInteger max;

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