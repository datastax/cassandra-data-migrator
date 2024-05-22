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

import com.datastax.cdm.properties.PropertyHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SplitPartitionsTest {
    @AfterEach
    void tearDown() {
        PropertyHelper.destroyInstance();
    }

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
    void getSubPartitionsFromFileNoFileTest() throws IOException {
        Exception excp = assertThrows(RuntimeException.class,
                () -> SplitPartitions.getSubPartitionsFromFile(8, "partfile.csv"));
        assertTrue(excp.getMessage().contains("No 'partfile.csv' file found!! Add this file in the current folder & rerun!"));
    }

    @Test
    void getSubPartitionsFromFileTest() throws IOException {
        List<SplitPartitions.Partition> partitions = SplitPartitions.getSubPartitionsFromFile(5, "./src/resources/partitions.csv");
        assertEquals(25, partitions.size());
    }

    @Test
    void getSubPartitionsFromHighNumPartTest() throws IOException {
        List<SplitPartitions.Partition> partitions = SplitPartitions.getSubPartitionsFromFile(1000, "./src/resources/partitions.csv");
        assertEquals(50, partitions.size());
    }


    @Test
    void batchesTest() {
        List<String> mutable_list = Arrays.asList("e1", "e2", "e3", "e4", "e5", "e6");
        Stream<List<String>> out = SplitPartitions.batches(mutable_list, 2);
        assertEquals(3, out.count());
    }

    @Test
    void PartitionMinMaxBlankTest() {
        assertTrue((new SplitPartitions.PartitionMinMax("")).hasError);
    }

    @Test
    void PartitionMinMaxInvalidTest() {
        assertTrue((new SplitPartitions.PartitionMinMax(" # min, max")).hasError);
    }

    @Test
    void PartitionMinMaxValidTest() {
        assertTrue(!(new SplitPartitions.PartitionMinMax(" -123, 456")).hasError);
    }

    @Test
    void PartitionMinMaxValidMinMaxTest() {
        assertEquals(BigInteger.valueOf(-507900353496146534l), (new SplitPartitions.PartitionMinMax(" -507900353496146534, 456")).min);
        assertEquals(BigInteger.valueOf(9101008634499147643l), (new SplitPartitions.PartitionMinMax(" -507900353496146534,9101008634499147643")).max);
    }

    @Test
    void appendPartitionOnDiff() {
        PropertyHelper helper = PropertyHelper.getInstance();
        assertFalse(SplitPartitions.appendPartitionOnDiff(helper));
        helper.setProperty("spark.cdm.tokenrange.partitionFile.appendOnDiff", true);
        assertTrue(SplitPartitions.appendPartitionOnDiff(helper));
    }

    @Test
    void getPartitionFileInput() {
        PropertyHelper helper = PropertyHelper.getInstance();
        helper.setProperty("spark.cdm.schema.origin.keyspaceTable", "tb");
        assertEquals("./tb_partitions.csv", SplitPartitions.getPartitionFileInput(helper));

        helper.setProperty("spark.cdm.tokenrange.partitionFile", "./file.csv");
        assertEquals("./file.csv", SplitPartitions.getPartitionFileInput(helper));

        helper.setProperty("spark.cdm.tokenrange.partitionFile.input", "./file_input.csv");
        assertEquals("./file_input.csv", SplitPartitions.getPartitionFileInput(helper));
    }

    @Test
    void getPartitionFileOutput() {
        PropertyHelper helper = PropertyHelper.getInstance();
        helper.setProperty("spark.cdm.schema.origin.keyspaceTable", "tb");
        assertEquals("./tb_partitions.csv", SplitPartitions.getPartitionFileOutput(helper));

        helper.setProperty("spark.cdm.tokenrange.partitionFile", "./file.csv");
        assertEquals("./file.csv", SplitPartitions.getPartitionFileOutput(helper));

        helper.setProperty("spark.cdm.tokenrange.partitionFile.output", "./file_output.csv");
        assertEquals("./file_output.csv", SplitPartitions.getPartitionFileOutput(helper));
    }
}
