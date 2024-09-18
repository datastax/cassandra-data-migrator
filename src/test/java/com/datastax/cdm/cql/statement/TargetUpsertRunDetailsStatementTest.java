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
package com.datastax.cdm.cql.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.job.RunNotStartedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class TargetUpsertRunDetailsStatementTest extends CommonMocks {
    @Mock
    PreparedStatement preparedStatement;

    @Mock
    CqlSession cqlSession;

    @Mock
    ResultSet rs;

    @Mock
    Row row;

    @Mock
    BoundStatement bStatement;

    TargetUpsertRunDetailsStatement targetUpsertRunDetailsStatement;

    @BeforeEach
    public void setup() {
        // UPDATE is needed by counters, though the class should handle non-counter updates
        commonSetup(false, false, true);
        when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(bStatement);
        when(cqlSession.execute(bStatement)).thenReturn(rs);
        when(rs.all()).thenReturn(List.of(row));
    }

    @Test
    public void init() throws RunNotStartedException {
        targetUpsertRunDetailsStatement = new TargetUpsertRunDetailsStatement(cqlSession, "ks.table1");
        assertEquals(Collections.emptyList(), targetUpsertRunDetailsStatement.getPendingPartitions(0));
    }

    @Test
    public void incorrectKsTable() throws RunNotStartedException {
        assertThrows(RuntimeException.class, () -> new TargetUpsertRunDetailsStatement(cqlSession, "table1"));
    }

}
