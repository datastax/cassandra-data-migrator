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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.datastax.cdm.job.IJobSessionFactory.JobType;

/**
 * The mid-partition flush bounds how many async writes are in flight, so it is the only back-pressure the copy path
 * has. It reads the UNFLUSHED counter, which {@link JobCounter} keeps in two places: an interim value advanced by
 * {@code increment()}, and an aggregate advanced only by {@code flush()} (called once per partition range). These tests
 * pin the threshold to the interim value, so the flush happens while the range is still being copied.
 */
class CopyJobSessionFlushTest {

    private static final int THRESHOLD = 100;

    private JobCounter migrateCounter() {
        return new JobCounter(JobType.MIGRATE);
    }

    @Test
    void belowThreshold_doesNotFlush() {
        JobCounter jc = migrateCounter();
        for (int i = 0; i < THRESHOLD - 1; i++) {
            jc.increment(JobCounter.CounterType.UNFLUSHED);
        }

        assertFalse(CopyJobSession.shouldFlush(jc, THRESHOLD), "99 writes in flight must not trigger a flush");
    }

    @Test
    void atThreshold_flushesWithinTheRange() {
        JobCounter jc = migrateCounter();
        for (int i = 0; i < THRESHOLD; i++) {
            jc.increment(JobCounter.CounterType.UNFLUSHED);
        }

        assertTrue(CopyJobSession.shouldFlush(jc, THRESHOLD),
                "the threshold must be reachable without an intervening jobCounter.flush()");
        assertEquals(0, jc.getCount(JobCounter.CounterType.UNFLUSHED),
                "the aggregate counter stays 0 mid-range, which is why the threshold must read the interim value");
    }

    @Test
    void afterReset_startsCountingAgain() {
        JobCounter jc = migrateCounter();
        for (int i = 0; i < THRESHOLD; i++) {
            jc.increment(JobCounter.CounterType.UNFLUSHED);
        }
        jc.increment(JobCounter.CounterType.WRITE, jc.getCount(JobCounter.CounterType.UNFLUSHED, true));
        jc.reset(JobCounter.CounterType.UNFLUSHED);

        assertFalse(CopyJobSession.shouldFlush(jc, THRESHOLD), "after a flush the in-flight count restarts from zero");

        for (int i = 0; i < THRESHOLD; i++) {
            jc.increment(JobCounter.CounterType.UNFLUSHED);
        }
        assertTrue(CopyJobSession.shouldFlush(jc, THRESHOLD), "the next batch of writes must flush as well");
    }

    @Test
    void wholeRangeNeverStaysInFlight() {
        // Mirrors the copy loop over a large partition range (13.4k rows, as produced by numParts sized for ~10MB
        // parts on a table whose primary key is the partition key, i.e. batchSize=1 and one write per row).
        JobCounter jc = migrateCounter();
        int flushes = 0;
        for (int row = 0; row < 13_400; row++) {
            jc.increment(JobCounter.CounterType.UNFLUSHED);
            if (CopyJobSession.shouldFlush(jc, THRESHOLD)) {
                jc.increment(JobCounter.CounterType.WRITE, jc.getCount(JobCounter.CounterType.UNFLUSHED, true));
                jc.reset(JobCounter.CounterType.UNFLUSHED);
                flushes++;
            }
        }

        assertEquals(134, flushes, "13.4k rows at a threshold of 100 must flush 134 times");
        assertTrue(jc.getCount(JobCounter.CounterType.UNFLUSHED, true) < THRESHOLD,
                "no more than a threshold's worth of writes may remain in flight at any point");
    }
}
