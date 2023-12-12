package com.datastax.cdm.job;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SplitPartitionsTest {

    @Test
    void getRandomSubPartitionsTest() {
        List<SplitPartitions.Partition> partitions = SplitPartitions.getRandomSubPartitions(10, BigInteger.ONE,
                BigInteger.valueOf(100), 100);
        assertEquals(10, partitions.size());
        partitions.forEach(p -> {
            assertEquals(9, p.getMax().longValue() - p.getMin().longValue());
        });
    }

    @Test
    void getRandomSubPartitionsTestOver100() {
        List<SplitPartitions.Partition> partitions = SplitPartitions.getRandomSubPartitions(8, BigInteger.ONE,
                BigInteger.valueOf(44), 200);
        assertEquals(8, partitions.size());
    }
    @Test
    void batchesTest() {
        List<String> mutable_list = Arrays.asList("e1", "e2", "e3", "e4", "e5", "e6");
        Stream<List<String>> out = SplitPartitions.batches(mutable_list, 2);
        assertEquals(3, out.count());
    }

}
