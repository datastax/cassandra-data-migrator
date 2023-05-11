package com.datastax.cdm.cql.statement;

import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.cdm.cql.CommonMocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TargetSelectByPKStatementTest extends CommonMocks {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    TargetSelectByPKStatement targetSelectByPKStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        targetSelectByPKStatement = new TargetSelectByPKStatement(propertyHelper, targetSession);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(String.join(",",targetColumnNames))
                .append(" FROM ")
                .append(targetKeyspaceTableName)
                .append(" WHERE ")
                .append(keyEqualsBindJoinedWithAND(targetPrimaryKey));

        assertEquals(sb.toString(), targetSelectByPKStatement.getCQL());
    }

    @Test
    public void cql_withConstantColumnInKey() {
        String constKeyCol = constantColumns.get(0);
        String constKeyVal = constantColumnValues.get(0);
        targetClusteringKey.add(constKeyCol);
        targetClusteringKeyTypes.add(constantColumnTypes.get(0));
        commonSetup(false,true,false);
        targetSelectByPKStatement = new TargetSelectByPKStatement(propertyHelper, targetSession);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(String.join(",",targetColumnNames))
                .append(" FROM ")
                .append(targetKeyspaceTableName)
                .append(" WHERE ");

        for (int i=0; i<targetPrimaryKey.size(); i++) {
            if (i >0) {
                sb.append(" AND ");
            }
            String key = targetPrimaryKey.get(i);
            if (key.equals(constKeyCol)) {
                sb.append(key).append("=").append(constKeyVal);
            } else {
                sb.append(key).append("=?");
            }
        }

        logger.info("CQL: " + sb.toString());
        assertEquals(sb.toString(), targetSelectByPKStatement.getCQL());
    }

    @Test
    public void getRecord() {
        targetSelectByPKStatement.getRecord(pk);
        assertAll(
                () -> verify(preparedStatement).bind(),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(pkFactory).bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, 0)
        );
    }

    @Test
    public void getAsyncResult() {
        targetSelectByPKStatement.getAsyncResult(pk);
        verify(targetCqlSession).executeAsync(boundStatement);
    }

    @Test
    public void getRecord_nullBoundStatement() {
        when(pkFactory.bindWhereClause(any(PKFactory.Side.class), any(EnhancedPK.class), eq(boundStatement), anyInt())).thenReturn(null);
        assertNull(targetSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void getRecord_nullResultSet() {
        when(targetCqlSession.execute(any(BoundStatement.class))).thenReturn(null);
        assertNull(targetSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void getRecord_nullRow() {
        when(targetResultSet.one()).thenReturn(null);
        assertNull(targetSelectByPKStatement.getRecord(pk));
    }

    @Test
    public void getAsyncResult_nullBoundStatement() {
        when(pkFactory.bindWhereClause(any(PKFactory.Side.class), any(EnhancedPK.class), eq(boundStatement), anyInt())).thenReturn(null);
        assertNull(targetSelectByPKStatement.getAsyncResult(pk));
    }

}
