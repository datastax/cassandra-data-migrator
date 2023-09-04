package com.datastax.cdm.job;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobCounter {

    // Enumeration for counter types
    public enum CounterType {
        READ, WRITE, VALID, ERROR, MISMATCH, MISSING, CORRECTED_MISSING, CORRECTED_MISMATCH, SKIPPED, UNFLUSHED, LARGE
    }

    // Logger instance
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    // Internal class to handle atomic counting operations
    private static class CounterUnit {
        private final AtomicLong globalCounter = new AtomicLong(0);
        private final ThreadLocal<Long> threadLocalCounter = ThreadLocal.withInitial(() -> 0L);

        public void incrementThreadLocal(long incrementBy) {
            threadLocalCounter.set(threadLocalCounter.get() + incrementBy);
        }

        public long getThreadLocal() {
            return threadLocalCounter.get();
        }

        public void resetThreadLocal() {
            threadLocalCounter.set(0L);
        }

        public void setGlobalCounter(long value) {
            globalCounter.set(value);
        }

        public void addThreadLocalToGlobalCounter() {
            globalCounter.addAndGet(threadLocalCounter.get());
        }

        public long getGlobalCounter() {
            return globalCounter.get();
        }
    }

    // Declare individual counters for different operations
    private final CounterUnit readCounter = new CounterUnit();
    private final CounterUnit mismatchCounter = new CounterUnit();
    private final CounterUnit missingCounter = new CounterUnit();
    private final CounterUnit correctedMissingCounter = new CounterUnit();
    private final CounterUnit correctedMismatchCounter = new CounterUnit();
    private final CounterUnit validCounter = new CounterUnit();
    private final CounterUnit skippedCounter = new CounterUnit();
    private final CounterUnit writeCounter = new CounterUnit();
    private final CounterUnit errorCounter = new CounterUnit();
    private final CounterUnit unflushedCounter = new CounterUnit();
    private final CounterUnit largeCounter = new CounterUnit();

    // Variables to hold lock objects and registered types
    private final Object globalLock = new Object();
    private Set<CounterType> registeredTypes;
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
        this.registeredTypes = EnumSet.noneOf(CounterType.class);
        Collections.addAll(this.registeredTypes, registeredTypes);
    }

    // Utility method to fetch the appropriate counter unit based on type
    private CounterUnit getCounterUnit(CounterType counterType) {
        if (!registeredTypes.contains(counterType)) {
            throw new IllegalArgumentException("CounterType " + counterType + " is not registered");
        }
        switch (counterType) {
            case READ: return readCounter;
            case VALID: return validCounter;
            case MISMATCH: return mismatchCounter;
            case MISSING: return missingCounter;
            case CORRECTED_MISSING: return correctedMissingCounter;
            case CORRECTED_MISMATCH: return correctedMismatchCounter;
            case SKIPPED: return skippedCounter;
            case WRITE: return writeCounter;
            case ERROR: return errorCounter;
            case UNFLUSHED: return unflushedCounter;
            case LARGE: return largeCounter;
            default: throw new IllegalArgumentException("Unknown CounterType: " + counterType);
        }
    }

    // Method to get a counter's value
    public long getCount(CounterType counterType, boolean global) {
        return global ? getCounterUnit(counterType).getGlobalCounter() : getCounterUnit(counterType).getThreadLocal();
    }

    // Method to get a thread counter's value
    public long getCount(CounterType counterType) {
        return getCount(counterType, false);
    }

    // Method to reset thread-specific counters for given type
    public void threadReset(CounterType counterType) {
        getCounterUnit(counterType).resetThreadLocal();
    }

    // Method to reset thread-specific counters for all registered types
    public void threadReset() {
        for (CounterType type : registeredTypes) {
            threadReset(type);
        }
    }

    // Method to increment thread-specific counters by a given value
    public void threadIncrement(CounterType counterType, long incrementBy) {
        getCounterUnit(counterType).incrementThreadLocal(incrementBy);
    }

    // Method to increment thread-specific counters by 1
    public void threadIncrement(CounterType counterType) {
        threadIncrement(counterType, 1);
    }

    // Method to increment global counters based on thread-specific counters
    public void globalIncrement() {
        synchronized (globalLock) {
            for (CounterType type : registeredTypes) {
                getCounterUnit(type).addThreadLocalToGlobalCounter();
            }
        }
    }

    // Method to get current counts (both thread-specific and global) as a formatted string
    public String getThreadCounters(boolean global) {
        StringBuilder sb = new StringBuilder();
        for (CounterType type : registeredTypes) {
            long value = global ? getCounterUnit(type).getGlobalCounter() : getCounterUnit(type).getThreadLocal();
            sb.append(type.name()).append("=").append(value).append(", ");
        }
        // Remove the trailing comma and space
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
        }
        return sb.toString();
    }

    public void printProgress() {
        printProgress(false);
        printProgress(true);
    }

    // Determines if it's the right time to print global progress
    protected boolean shouldPrintGlobalProgress() {
        if (!registeredTypes.contains(CounterType.READ)) {
            return false;
        }
        long globalReads = readCounter.getGlobalCounter();
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

    // Method to print the current progress
    public void printProgress(boolean global) {
        if (global) {
            if (shouldPrintGlobalProgress()) {
                printAndLogProgress("Progress Counts: ", true);
            }
        } else {
            if (printPerThread) {
                printAndLogProgress("Thread Counts: ", false);
            }
        }
    }

    public void printFinal() {
        logger.info("################################################################################################");
        if (registeredTypes.contains(CounterType.READ))               logger.info("Final Read Record Count: {}", readCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.MISMATCH))           logger.info("Final Mismatch Record Count: {}", mismatchCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.CORRECTED_MISMATCH)) logger.info("Final Corrected Mismatch Record Count: {}", correctedMismatchCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.MISSING))            logger.info("Final Missing Record Count: {}", missingCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.CORRECTED_MISSING))  logger.info("Final Corrected Missing Record Count: {}", correctedMissingCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.VALID))              logger.info("Final Valid Record Count: {}", validCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.SKIPPED))            logger.info("Final Skipped Record Count: {}", skippedCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.WRITE))              logger.info("Final Write Record Count: {}", writeCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.ERROR))              logger.info("Final Error Record Count: {}", errorCounter.getGlobalCounter());
        if (registeredTypes.contains(CounterType.LARGE))              logger.info("Final Large Record Count: {}", largeCounter.getGlobalCounter());
        logger.info("################################################################################################");
    }

}
