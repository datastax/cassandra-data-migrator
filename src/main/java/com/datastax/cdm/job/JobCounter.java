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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.TrackRun;

public class JobCounter {

	// Enumeration for counter types
	public enum CounterType {
		READ, WRITE, VALID, ERROR, MISMATCH, MISSING, CORRECTED_MISSING, CORRECTED_MISMATCH, SKIPPED, UNFLUSHED, LARGE
	}

	// Logger instance
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	// Internal class to handle atomic counting operations
	private static class CounterUnit {
		private final AtomicLong globalCounter = new AtomicLong(0);
		private final ThreadLocal<Long> threadLocalCounter = ThreadLocal.withInitial(() -> 0L);

		public void incrementThreadCounter(long incrementBy) {
			threadLocalCounter.set(threadLocalCounter.get() + incrementBy);
		}

		public long getThreadCounter() {
			return threadLocalCounter.get();
		}

		public void resetThreadCounter() {
			threadLocalCounter.set(0L);
		}

		public void setGlobalCounter(long value) {
			globalCounter.set(value);
		}

		public void addThreadToGlobalCounter() {
			globalCounter.addAndGet(threadLocalCounter.get());
		}

		public long getGlobalCounter() {
			return globalCounter.get();
		}
	}

	// Declare individual counters for different operations
	private final HashMap<CounterType, CounterUnit> counterMap = new HashMap<>();

	// Variables to hold lock objects and registered types
	private final Object globalLock = new Object();
	private final boolean printPerThread;
	private final long printStatsAfter;
	private final CounterUnit printCounter = new CounterUnit();

	// Constructor
	public JobCounter(long printStatsAfter, boolean printStatsPerPart) {
		this.printStatsAfter = printStatsAfter;
		this.printPerThread = printStatsPerPart;
	}

	// Allows setting the registered counter types.
	public void setRegisteredTypes(CounterType... registeredTypes) {
		counterMap.clear();
		for (CounterType type : registeredTypes) {
			counterMap.put(type, new CounterUnit());
		}
	}

	// Utility method to fetch the appropriate counter unit based on type
	private CounterUnit getCounterUnit(CounterType counterType) {
		if (!counterMap.containsKey(counterType)) {
			throw new IllegalArgumentException("CounterType " + counterType + " is not registered");
		}
		return (counterMap.get(counterType));
	}

	// Method to get a counter's value
	public long getCount(CounterType counterType, boolean global) {
		return global ? getCounterUnit(counterType).getGlobalCounter() : getCounterUnit(counterType).getThreadCounter();
	}

	// Method to get a thread counter's value
	public long getCount(CounterType counterType) {
		return getCount(counterType, false);
	}

	// Method to reset thread-specific counters for given type
	public void threadReset(CounterType counterType) {
		getCounterUnit(counterType).resetThreadCounter();
	}

	// Method to reset thread-specific counters for all registered types
	public void threadReset() {
		for (CounterType type : counterMap.keySet()) {
			threadReset(type);
		}
	}

	// Method to increment thread-specific counters by a given value
	public void threadIncrement(CounterType counterType, long incrementBy) {
		getCounterUnit(counterType).incrementThreadCounter(incrementBy);
	}

	// Method to increment thread-specific counters by 1
	public void threadIncrement(CounterType counterType) {
		threadIncrement(counterType, 1);
	}

	// Method to increment global counters based on thread-specific counters
	public void globalIncrement() {
		synchronized (globalLock) {
			for (CounterType type : counterMap.keySet()) {
				getCounterUnit(type).addThreadToGlobalCounter();
			}
		}
	}

	// Method to get current counts (both thread-specific and global) as a formatted
	// string
	public String getThreadCounters(boolean global) {
		StringBuilder sb = new StringBuilder();
		for (CounterType type : counterMap.keySet()) {
			long value = global ? getCounterUnit(type).getGlobalCounter() : getCounterUnit(type).getThreadCounter();
			sb.append(type.name()).append("=").append(value).append(", ");
		}
		// Remove the trailing comma and space
		if (sb.length() > 2) {
			sb.setLength(sb.length() - 2);
		}
		return sb.toString();
	}

	public void printProgress() {
		if (printPerThread) {
			printAndLogProgress("Thread Counts: ", false);
		} else if (shouldPrintGlobalProgress()) {
			printAndLogProgress("Progress Counts: ", true);
		}
	}

	// Determines if it's the right time to print global progress
	protected boolean shouldPrintGlobalProgress() {
		if (!counterMap.containsKey(CounterType.READ)) {
			return false;
		}
		long globalReads = counterMap.get(CounterType.READ).getGlobalCounter();
		long expectedPrintCount = globalReads - globalReads % printStatsAfter;
		if (expectedPrintCount > printCounter.getGlobalCounter()) {
			printCounter.setGlobalCounter(expectedPrintCount);
			return true;
		}
		return false;
	}

	// Prints and logs the progress
	protected void printAndLogProgress(String message, boolean global) {
		String fullMessage = message + getThreadCounters(global);
		logger.info(fullMessage);
	}

	public void printFinal(boolean trackRun, TrackRun trackRunFeature) {
		if (trackRun && null != trackRunFeature) {
			StringBuilder sb = new StringBuilder();
			if (counterMap.containsKey(CounterType.READ))
				sb.append("Read: " + counterMap.get(CounterType.READ).getGlobalCounter());
			if (counterMap.containsKey(CounterType.MISMATCH))
				sb.append("; Mismatch: " + counterMap.get(CounterType.MISMATCH).getGlobalCounter());
			if (counterMap.containsKey(CounterType.CORRECTED_MISMATCH))
				sb.append("; Corrected Mismatch: " + counterMap.get(CounterType.CORRECTED_MISMATCH).getGlobalCounter());
			if (counterMap.containsKey(CounterType.MISSING))
				sb.append("; Missing: " + counterMap.get(CounterType.MISSING).getGlobalCounter());
			if (counterMap.containsKey(CounterType.CORRECTED_MISSING))
				sb.append("; Corrected Missing: " + counterMap.get(CounterType.CORRECTED_MISSING).getGlobalCounter());
			if (counterMap.containsKey(CounterType.VALID))
				sb.append("; Valid: " + counterMap.get(CounterType.VALID).getGlobalCounter());
			if (counterMap.containsKey(CounterType.SKIPPED))
				sb.append("; Skipped: " + counterMap.get(CounterType.SKIPPED).getGlobalCounter());
			if (counterMap.containsKey(CounterType.WRITE))
				sb.append("; Write: " + counterMap.get(CounterType.WRITE).getGlobalCounter());
			if (counterMap.containsKey(CounterType.ERROR))
				sb.append("; Error: " + counterMap.get(CounterType.ERROR).getGlobalCounter());
			if (counterMap.containsKey(CounterType.LARGE))
				sb.append("; Large: " + counterMap.get(CounterType.LARGE).getGlobalCounter());

			trackRunFeature.endCdmRun(sb.toString());
		}
		logger.info("################################################################################################");
		if (counterMap.containsKey(CounterType.READ))
			logger.info("Final Read Record Count: {}", counterMap.get(CounterType.READ).getGlobalCounter());
		if (counterMap.containsKey(CounterType.MISMATCH))
			logger.info("Final Mismatch Record Count: {}", counterMap.get(CounterType.MISMATCH).getGlobalCounter());
		if (counterMap.containsKey(CounterType.CORRECTED_MISMATCH))
			logger.info("Final Corrected Mismatch Record Count: {}",
					counterMap.get(CounterType.CORRECTED_MISMATCH).getGlobalCounter());
		if (counterMap.containsKey(CounterType.MISSING))
			logger.info("Final Missing Record Count: {}", counterMap.get(CounterType.MISSING).getGlobalCounter());
		if (counterMap.containsKey(CounterType.CORRECTED_MISSING))
			logger.info("Final Corrected Missing Record Count: {}",
					counterMap.get(CounterType.CORRECTED_MISSING).getGlobalCounter());
		if (counterMap.containsKey(CounterType.VALID))
			logger.info("Final Valid Record Count: {}", counterMap.get(CounterType.VALID).getGlobalCounter());
		if (counterMap.containsKey(CounterType.SKIPPED))
			logger.info("Final Skipped Record Count: {}", counterMap.get(CounterType.SKIPPED).getGlobalCounter());
		if (counterMap.containsKey(CounterType.WRITE))
			logger.info("Final Write Record Count: {}", counterMap.get(CounterType.WRITE).getGlobalCounter());
		if (counterMap.containsKey(CounterType.ERROR))
			logger.info("Final Error Record Count: {}", counterMap.get(CounterType.ERROR).getGlobalCounter());
		if (counterMap.containsKey(CounterType.LARGE))
			logger.info("Final Large Record Count: {}", counterMap.get(CounterType.LARGE).getGlobalCounter());
		logger.info("################################################################################################");
	}

}
