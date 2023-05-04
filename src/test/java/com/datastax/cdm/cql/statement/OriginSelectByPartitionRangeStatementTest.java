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
    public void bind_withNullBinds_usesDefaultPartitions() {
        originSelectByPartitionRangeStatement.bind(null, null);
        assertAll(
                () -> verify(preparedStatement).bind(
                        BigInteger.valueOf(minPartition).longValueExact(),
                        BigInteger.valueOf(maxPartition).longValueExact()),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }

    @Test
    public void bind_withNonNullBinds_usesProvidedPartitions() {
        BigInteger providedMin = BigInteger.valueOf(12345L);
        BigInteger providedMax = BigInteger.valueOf(67890L);

        originSelectByPartitionRangeStatement.bind(providedMin, providedMax);
        assertAll(
                () -> verify(preparedStatement).bind(
                        providedMin.longValueExact(),
                        providedMax.longValueExact()),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }

    @Test
    public void bind_withMixedBinds_usesDefaultAndProvidedPartitions() {
        BigInteger providedMin = BigInteger.valueOf(12345L);

        originSelectByPartitionRangeStatement.bind(providedMin, null);
        assertAll(
                () -> verify(preparedStatement).bind(
                        providedMin.longValueExact(),
                        BigInteger.valueOf(maxPartition).longValueExact()),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }

    @Test
    public void bind_withNullBinds_usesDefaultPartitions_whenRandomPartitioner() {
        when(originTable.hasRandomPartitioner()).thenReturn(true);

        originSelectByPartitionRangeStatement.bind(null, null);
        assertAll(
                () -> verify(preparedStatement).bind(
                        BigInteger.valueOf(minPartition),
                        BigInteger.valueOf(maxPartition)),
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
    public void bind_withMixedBinds_usesDefaultAndProvidedPartitions_whenRandomPartitioner() {
        when(originTable.hasRandomPartitioner()).thenReturn(true);

        BigInteger providedMax = BigInteger.valueOf(999999999L);

        originSelectByPartitionRangeStatement.bind(null, providedMax);
        assertAll(
                () -> verify(preparedStatement).bind(
                        BigInteger.valueOf(minPartition),
                        providedMax),
                () -> verify(boundStatement).setConsistencyLevel(readCL),
                () -> verify(boundStatement).setPageSize(fetchSizeInRows)
        );
    }

    @Test
    public void bind_withInvalidBindLength_throwsException() {
        assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind(null));
    }

    @Test
    public void bind_withInvalidBindType_throwsException() {
        assertThrows(RuntimeException.class, () -> originSelectByPartitionRangeStatement.bind("invalidType", BigInteger.valueOf(20)));
    }

}
