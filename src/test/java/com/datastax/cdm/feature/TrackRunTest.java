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
package com.datastax.cdm.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.cql.statement.TargetUpsertRunDetailsStatement;
import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.job.PartitionRange;
import com.datastax.cdm.job.RunNotStartedException;

class TrackRunTest extends CommonMocks {

    @Mock
    private TargetUpsertRunDetailsStatement runStatement;

    TrackRun trackRun;

    @BeforeEach
    public void setup() throws RunNotStartedException {
        // UPDATE is needed by counters, though the class should handle non-counter
        // updates
        commonSetup(false, false, true);
        trackRun = new TrackRun(originCqlSession, "ks.table");
    }

    @Test
    void countTypesAndStatus() {
        assertEquals("MIGRATE", JobType.MIGRATE.name());
        assertEquals("VALIDATE", JobType.VALIDATE.name());

        assertEquals(3, JobType.values().length);
        assertEquals(7, TrackRun.RUN_STATUS.values().length);
    }

    @Test
    void getPendingPartitions() throws RunNotStartedException {
        Collection<PartitionRange> parts = trackRun.getPendingPartitions(0L, JobType.MIGRATE, 1);
        assertEquals(0, parts.size());
    }

    @Test
    void getPendingPartitionsRerun() throws RunNotStartedException {
        Collection<PartitionRange> parts = trackRun.getPendingPartitionsWithMultiplier(JobType.MIGRATE, 4,
                createPartitionRanges());
        assertEquals(8, parts.size());
    }

    private Collection<PartitionRange> createPartitionRanges() {
        Collection<PartitionRange> parts = new java.util.ArrayList<>();
        parts.add(new PartitionRange(java.math.BigInteger.valueOf(0), java.math.BigInteger.valueOf(100),
                JobType.MIGRATE));
        parts.add(new PartitionRange(java.math.BigInteger.valueOf(101), java.math.BigInteger.valueOf(200),
                JobType.MIGRATE));
        return parts;
    }

}
