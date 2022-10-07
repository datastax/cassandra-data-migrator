package datastax.astra.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SplitPartitions {

    public final static Long MIN_PARTITION = Long.MIN_VALUE;
    public final static Long MAX_PARTITION = Long.MAX_VALUE;
    public static Logger logger = LoggerFactory.getLogger(SplitPartitions.class.getName());

    public static void main(String[] args) {
        Collection<Partition> partitions = getSubPartitions(new BigInteger("20"), BigInteger.valueOf(MIN_PARTITION),
                BigInteger.valueOf(MAX_PARTITION), 20);
        for (Partition partition : partitions) {
            System.out.println(partition);
        }
    }

    public static Collection<Partition> getRandomSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max, int coveragePercent) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Splitting min: " + min + " max:" + max);
        List<Partition> partitions = getSubPartitions(splitSize, min, max, coveragePercent);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        Collections.shuffle(partitions);
        return partitions;
    }

    private static List<Partition> getSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max, int coveragePercent) {
        if (coveragePercent < 1 || coveragePercent > 100) {
            coveragePercent = 100;
        }
        BigInteger curMax = new BigInteger(min.toString());
        BigInteger partitionSize = max.subtract(min).divide(splitSize);
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
        }

        return partitions;
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