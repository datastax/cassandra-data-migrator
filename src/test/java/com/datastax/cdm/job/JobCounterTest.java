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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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
    public void testThreadIncrement() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        assertEquals(5, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void testGlobalIncrement() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        jobCounter.globalIncrement();
        assertEquals(5, jobCounter.getCount(JobCounter.CounterType.READ, true));
    }

    @Test
    public void testThreadResetForSpecificType() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        jobCounter.threadReset(JobCounter.CounterType.READ);
        assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
    }

    @Test
    public void testUnregisteredCounterType() {
        JobCounter localJobCounter = new JobCounter(JobType.MIGRATE);
        assertThrows(IllegalArgumentException.class,
                () -> localJobCounter.threadIncrement(JobCounter.CounterType.CORRECTED_MISMATCH, 5));
    }

    @Test
    public void testPrintProgressForGlobalAndThread() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 11);
        jobCounter.globalIncrement();
        // You may use mocking to capture logger outputs
        jobCounter.printMetrics(0, null);
    }

    @Test
    public void testPrintFinal() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        jobCounter.globalIncrement();
        // You may use mocking to capture logger outputs
        jobCounter.printMetrics(0, null);
    }

    @Captor
    private ArgumentCaptor<Long> trackRunInfoCaptorLong;

    @Captor
    private ArgumentCaptor<String> trackRunInfoCaptor;

    // @Test
    // public void testPrintFinalWithRunTracking() {
    // jobCounter = new JobCounter(JobType.VALIDATE);
    //
    // String expected = "Read: 5; Mismatch: 0; Corrected Mismatch: 0; Missing: 7; Corrected Missing: 7; Valid: 0;
    // Skipped: 0; Error: 72";
    // jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
    // jobCounter.threadIncrement(JobCounter.CounterType.CORRECTED_MISSING, 7);
    // jobCounter.threadIncrement(JobCounter.CounterType.ERROR, 72);
    // jobCounter.threadIncrement(JobCounter.CounterType.MISSING, 7);
    // jobCounter.globalIncrement();
    // // You may use mocking to capture logger outputs
    // jobCounter.printMetrics(0, trackRun);
    // Mockito.verify(trackRun).endCdmRun(trackRunInfoCaptorLong.capture(), trackRunInfoCaptor.capture());
    // assertEquals(expected, trackRunInfoCaptor.getValue());
    // }

    @Test
    public void testGetCountGlobal() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        jobCounter.globalIncrement();
        assertEquals(5, jobCounter.getCount(JobCounter.CounterType.READ, true));
    }

    @Test
    public void threadIncrementByOne() {
        jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
        jobCounter.threadIncrement(JobCounter.CounterType.READ);
        assertEquals(6, jobCounter.getCount(JobCounter.CounterType.READ));
    }

}
