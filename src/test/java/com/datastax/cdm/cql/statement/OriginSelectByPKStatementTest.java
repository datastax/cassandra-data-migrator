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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

public class OriginSelectByPKStatementTest extends CommonMocks {

    OriginSelectByPKStatement originSelectByPKStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        originSelectByPKStatement = new OriginSelectByPKStatement(propertyHelper, originSession);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(String.join(",", originColumnNames)).append(" FROM ")
                .append(originKeyspaceTableName).append(" WHERE ").append(keyEqualsBindJoinedWithAND(originPrimaryKey));

        String cql = originSelectByPKStatement.getCQL();
        assertEquals(sb.toString(), cql);
    }

    @Test
    public void testGetRecord_success() {
        Record result = originSelectByPKStatement.getRecord(pk);
        assertNotNull(result);
        assertEquals(pk, result.getPk());
    }

    @Test
    public void testGetRecord_nullBoundStatement() {
        when(originSelectByPKStatement.bind(pk)).thenReturn(null);
        assertNull(originSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void testGetRecord_nullResultSet() {
        when(originCqlSession.execute(any(BoundStatement.class))).thenReturn(null);
        assertNull(originSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void testGetRecord_nullRow() {
        when(originResultSet.one()).thenReturn(null);
        assertNull(originSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void testBind_success() {
        originSelectByPKStatement.bind(pk);

        assertAll(() -> verify(preparedStatement).bind(), () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows),
                () -> verify(pkFactory).bindWhereClause(PKFactory.Side.ORIGIN, pk, boundStatement, 0));
    }

    @Test
    public void testBind_nullBinds() {
        assertThrows(RuntimeException.class, () -> originSelectByPKStatement.bind((Object[]) null));
    }

    @Test
    public void testBind_wrongNumberOfBinds() {
        assertThrows(RuntimeException.class, () -> originSelectByPKStatement.bind(pk, new Object()));
    }

    @Test
    public void testBind_wrongTypeOfBinds() {
        assertThrows(RuntimeException.class, () -> originSelectByPKStatement.bind(new Object()));
    }

    @Test
    public void testBind_nullPK() {
        assertThrows(RuntimeException.class, () -> originSelectByPKStatement.bind((EnhancedPK) null));
    }

}
