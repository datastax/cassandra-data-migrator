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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.job.IJobSessionFactory.JobType;

public class JobCounterTest {

    private JobCounter jobCounter;
    @Mock
    private TrackRun trackRun;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        jobCounter = new JobCounter(JobType.MIGRATE);
    }

    @Test
    public void interimIncrement() {
        jobCounter.increment(JobCounter.CounterType.READ, 5);
        assertEquals(5, jobCounter.getCount(JobCounter.CounterType.READ, true));
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void increment() {
        jobCounter.increment(JobCounter.CounterType.READ, 5);
        jobCounter.flush();
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ, true));
        assertEquals(5, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void incrementByOne() {
        jobCounter.increment(JobCounter.CounterType.READ, 5);
        jobCounter.increment(JobCounter.CounterType.READ);
        assertEquals(6, jobCounter.getCount(JobCounter.CounterType.READ, true));
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
        jobCounter.flush();
        assertEquals(6, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void resetForSpecificType() {
        jobCounter.increment(JobCounter.CounterType.READ, 5);
        jobCounter.reset(JobCounter.CounterType.READ);
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ, true));
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void testUnregisteredCounterType() {
        JobCounter localJobCounter = new JobCounter(JobType.MIGRATE);
        assertThrows(IllegalArgumentException.class,
                () -> localJobCounter.increment(JobCounter.CounterType.CORRECTED_MISMATCH, 5));
    }

    @Captor
    private ArgumentCaptor<Long> trackRunInfoCaptorLong;

    @Captor
    private ArgumentCaptor<String> trackRunInfoCaptor;

    @Test
    public void printMetricsMigrate() {
        jobCounter = new JobCounter(JobType.MIGRATE);

        String expected = "Read: 10; Write: 7; Skipped: 1; Error: 2; Partitions Passed: 3; Partitions Failed: 2";
        jobCounter.increment(JobCounter.CounterType.READ, 10);
        jobCounter.increment(JobCounter.CounterType.WRITE, 7);
        jobCounter.increment(JobCounter.CounterType.ERROR, 2);
        jobCounter.increment(JobCounter.CounterType.SKIPPED, 1);
        jobCounter.increment(JobCounter.CounterType.UNFLUSHED, 3);
        jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED, 3);
        jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED, 2);
        jobCounter.flush();
        // You may use mocking to capture logger outputs
        jobCounter.printMetrics(0, trackRun);
        Mockito.verify(trackRun).endCdmRun(trackRunInfoCaptorLong.capture(), trackRunInfoCaptor.capture());
        assertEquals(expected, trackRunInfoCaptor.getValue());
    }

    @Test
    public void printMetricsValidate() {
        jobCounter = new JobCounter(JobType.VALIDATE);

        String expected = "Read: 5; Mismatch: 0; Corrected Mismatch: 0; Missing: 7; Corrected Missing: 7; Valid: 0; Skipped: 0; Error: 72; Partitions Passed: 4; Partitions Failed: 1";
        jobCounter.increment(JobCounter.CounterType.READ, 5);
        jobCounter.increment(JobCounter.CounterType.CORRECTED_MISSING, 7);
        jobCounter.increment(JobCounter.CounterType.ERROR, 72);
        jobCounter.increment(JobCounter.CounterType.MISSING, 7);
        jobCounter.increment(JobCounter.CounterType.PARTITIONS_PASSED, 4);
        jobCounter.increment(JobCounter.CounterType.PARTITIONS_FAILED, 1);
        jobCounter.flush();
        // You may use mocking to capture logger outputs
        jobCounter.printMetrics(0, trackRun);
        Mockito.verify(trackRun).endCdmRun(trackRunInfoCaptorLong.capture(), trackRunInfoCaptor.capture());
        assertEquals(expected, trackRunInfoCaptor.getValue());
    }

    @Test
    public void add() {
        JobCounter localJobCounter = new JobCounter(JobType.MIGRATE);
        localJobCounter.increment(JobCounter.CounterType.READ, 4);
        localJobCounter.increment(JobCounter.CounterType.WRITE, 5);
        localJobCounter.increment(JobCounter.CounterType.SKIPPED, 6);
        localJobCounter.increment(JobCounter.CounterType.ERROR, 7);
        localJobCounter.flush();
        jobCounter.add(localJobCounter);

        assertAll(() -> {
            assertEquals(4, jobCounter.getCount(JobCounter.CounterType.READ));
            assertEquals(5, jobCounter.getCount(JobCounter.CounterType.WRITE));
            assertEquals(6, jobCounter.getCount(JobCounter.CounterType.SKIPPED));
            assertEquals(7, jobCounter.getCount(JobCounter.CounterType.ERROR));
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.UNFLUSHED));
        });
    }

    @Test
    public void reset() {
        jobCounter.reset();

        assertAll(() -> {
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.WRITE));
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.SKIPPED));
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.ERROR));
            assertEquals(0, jobCounter.getCount(JobCounter.CounterType.UNFLUSHED));
        });
    }

    @Test
    public void isZero() {
        assertTrue(jobCounter.isZero());

        jobCounter.increment(JobCounter.CounterType.READ, 5);
        assertFalse(jobCounter.isZero());
    }

    @Test
    public void initGuardrail() {
        JobCounter localJobCounter = new JobCounter(JobType.GUARDRAIL);
        assertThrows(IllegalArgumentException.class, () -> localJobCounter.increment(JobCounter.CounterType.WRITE, 5));
    }

}
