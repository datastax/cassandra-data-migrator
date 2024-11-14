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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.properties.PropertyHelper;

public class SplitPartitionsTest {
    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    void getRandomSubPartitionsTest() {
        List<PartitionRange> partitions = SplitPartitions.getRandomSubPartitions(10, BigInteger.ONE,
                BigInteger.valueOf(100), 100, JobType.MIGRATE);
        assertEquals(10, partitions.size());
        partitions.forEach(p -> {
            assertEquals(9, p.getMax().longValue() - p.getMin().longValue());
        });
    }

    @Test
    void getRandomSubPartitionsTestOver100() {
        List<PartitionRange> partitions = SplitPartitions.getRandomSubPartitions(8, BigInteger.ONE,
                BigInteger.valueOf(44), 200, JobType.MIGRATE);
        assertEquals(8, partitions.size());
    }

}
