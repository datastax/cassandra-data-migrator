/*
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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
