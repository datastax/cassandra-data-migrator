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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.feature.TrackRun;

public class JobCounterTest {

	private JobCounter jobCounter;
	@Mock
	private TrackRun trackRun;

	@BeforeEach
	public void setUp() {
		MockitoAnnotations.openMocks(this);

		jobCounter = new JobCounter(10, true); // Changed to true to test printPerThread
		jobCounter.setRegisteredTypes(JobCounter.CounterType.values());
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
	public void testThreadResetForAllTypes() {
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
		jobCounter.threadIncrement(JobCounter.CounterType.WRITE, 5);
		jobCounter.threadReset();
		assertEquals(0, jobCounter.getCount(JobCounter.CounterType.READ));
		assertEquals(0, jobCounter.getCount(JobCounter.CounterType.WRITE));
	}

	@Test
	public void testUnregisteredCounterType() {
		JobCounter localJobCounter = new JobCounter(10, true);
		localJobCounter.setRegisteredTypes(JobCounter.CounterType.READ);
		assertThrows(IllegalArgumentException.class,
				() -> localJobCounter.threadIncrement(JobCounter.CounterType.WRITE, 5));
	}

	@Test
	public void testShouldPrintGlobalProgress() {
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 11);
		jobCounter.globalIncrement();
		assertTrue(jobCounter.shouldPrintGlobalProgress()); // assuming printStatsAfter is set to 10
	}

	@Test
	public void testPrintProgressForGlobalAndThread() {
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 11);
		jobCounter.globalIncrement();
		// You may use mocking to capture logger outputs
		jobCounter.printProgress();
	}

	@Test
	public void testPrintFinal() {
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
		jobCounter.globalIncrement();
		// You may use mocking to capture logger outputs
		jobCounter.printFinal(null);
	}

	@Captor
	private ArgumentCaptor<String> trackRunInfoCaptor;

	@Test
	public void testPrintFinalWithRunTracking() {
		String expected = "Read: 5; Mismatch: 0; Corrected Mismatch: 0; Missing: 0; Corrected Missing: 7; Valid: 0; Skipped: 0; Write: 0; Error: 72; Large: 42";
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
		jobCounter.threadIncrement(JobCounter.CounterType.CORRECTED_MISSING, 7);
		jobCounter.threadIncrement(JobCounter.CounterType.ERROR, 72);
		jobCounter.threadIncrement(JobCounter.CounterType.LARGE, 42);
		jobCounter.globalIncrement();
		// You may use mocking to capture logger outputs
		jobCounter.printFinal(trackRun);
		Mockito.verify(trackRun).endCdmRun(trackRunInfoCaptor.capture());
		assertEquals(expected, trackRunInfoCaptor.getValue());
	}

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

	@Test
	public void testShouldPrintGlobalProgressWithSufficientReads() {
		// Increment global READ counter to go beyond the printStatsAfter threshold
		// (assume it's 10)
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 11);
		jobCounter.globalIncrement();

		// shouldPrintGlobalProgress should return true because there are enough READs
		assertTrue(jobCounter.shouldPrintGlobalProgress());
	}

	@Test
	public void testShouldPrintGlobalProgressWithInsufficientReads() {
		// Increment global READ counter to remain less than printStatsAfter threshold
		// (assume it's 10)
		jobCounter.threadIncrement(JobCounter.CounterType.READ, 5);
		jobCounter.globalIncrement();

		// shouldPrintGlobalProgress should return true because there are enough READs
		assertFalse(jobCounter.shouldPrintGlobalProgress());
	}

	@Test
	public void testShouldPrintGlobalProgressWithUnregisteredRead() {
		jobCounter = new JobCounter(10, true); // Changed to true to test printPerThread

		// Set only WRITE as the registered type
		jobCounter.setRegisteredTypes(JobCounter.CounterType.WRITE);

		// shouldPrintGlobalProgress should return false because READ is not registered
		assertFalse(jobCounter.shouldPrintGlobalProgress());
	}

}
