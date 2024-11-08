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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.job.PartitionRange;
import com.datastax.cdm.job.RunNotStartedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

class TrackRunTest extends CommonMocks {

    @Mock
    CqlSession cqlSession;

    @Mock
    BoundStatement bStatement;

    @BeforeEach
    public void setup() {
        // UPDATE is needed by counters, though the class should handle non-counter updates
        commonSetup(false, false, true);
        when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(bStatement);
    }

    @Test
    void countTypesAndStatus() {
        assertEquals("MIGRATE", JobType.MIGRATE.name());
        assertEquals("VALIDATE", JobType.VALIDATE.name());

        assertEquals(3, JobType.values().length);
        assertEquals(7, TrackRun.RUN_STATUS.values().length);
    }

    @Test
    void init() throws RunNotStartedException {
        TrackRun trackRun = new TrackRun(cqlSession, "keyspace.table");
        Collection<PartitionRange> parts = trackRun.getPendingPartitions(0, JobType.MIGRATE);
        assertEquals(0, parts.size());
    }

}
