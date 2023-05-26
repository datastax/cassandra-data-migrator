package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.CommonMocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class OriginSelectByPartitionRangeStatementTest extends CommonMocks {

    OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        originSelectByPartitionRangeStatement = new OriginSelectByPartitionRangeStatement(propertyHelper, originSession);
    }

    @Test
    public void smoke_basicCQL() {
        String keys = String.join(",", originPartitionKey);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(String.join(",", originColumnNames))
                .append(" FROM ")
                .append(originKeyspaceTableName)
                .append(" WHERE ")
                .append("TOKEN(").append(keys).append(") >= ? AND TOKEN(").append(keys).append(") <= ?")
                .append(" ALLOW FILTERING");

        String cql = originSelectByPartitionRangeStatement.getCQL();
        assertEquals(sb.toString(),cql);
    }

    @Test
    public void originFilterCondition() {
        String filter=" AND cluster_key = 'abc'";
        when(originFilterConditionFeature.getFilterCondition()).thenReturn(filter);
        originSelectByPartitionRangeStatement = new OriginSelectByPartitionRangeStatement(propertyHelper, originSession);

        String keys = String.join(",", originPartitionKey);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(String.join(",", originColumnNames))
                .append(" FROM ")
                .append(originKeyspaceTableName)
                .append(" WHERE ")
                .append("TOKEN(").append(keys).append(") >= ? AND TOKEN(").append(keys).append(") <= ?")
                .append(filter)
                .append(" ALLOW FILTERING");

        String cql = originSelectByPartitionRangeStatement.getCQL();
        assertEquals(sb.toString(),cql);
    }

    @Test
    public void bind_withNullBinds() {
        assertAll(
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(null,null), "two null"),
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(BigInteger.valueOf(20)), "missing second"),
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(BigInteger.valueOf(20),null), "null second"),
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(null,BigInteger.valueOf(20)), "null first")
        );
    }

    @Test
    public void bind_withNonNullBinds_usesProvidedPartitions() {
        BigInteger providedMin = BigInteger.valueOf(12345L);
        BigInteger providedMax = BigInteger.valueOf(67890L);

        originSelectByPartitionRangeStatement.bind(providedMin, providedMax);
        assertAll(
                () -> verify(preparedStatement).bind(providedMin.longValueExact(), providedMax.longValueExact()),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }


    @Test
    public void bind_withNonNullBinds_usesProvidedPartitions_whenRandomPartitioner() {
        when(originTable.hasRandomPartitioner()).thenReturn(true);

        BigInteger providedMin = BigInteger.valueOf(12345L);
        BigInteger providedMax = BigInteger.valueOf(67890L);

        originSelectByPartitionRangeStatement.bind(providedMin, providedMax);
        assertAll(
                () -> verify(preparedStatement).bind(
                        providedMin,
                        providedMax),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }

    @Test
    public void bind_withInvalidBindType_throwsException() {
        assertAll(
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind("invalidType", BigInteger.valueOf(20)), "invalid first"),
                () -> assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(BigInteger.valueOf(20),"invalidType"), "invalid second")
        );
    }

}
