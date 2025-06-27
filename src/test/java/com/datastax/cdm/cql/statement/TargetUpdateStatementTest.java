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
import static org.mockito.Mockito.*;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class TargetUpdateStatementTest extends CommonMocks {

    TargetUpdateStatement targetUpdateStatement;
    String updateCQLBeginning;
    String counterUpdateCQLEnding;

    @BeforeEach
    public void setup() {
        // UPDATE is needed by counters, though the class should handle non-counter updates
        commonSetup(false, false, true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        // Ensure prepareStatement().bind() returns the boundStatement mock
        when(targetUpdateStatement.prepareStatement()).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        // Chain set and unset to return boundStatement
        when(boundStatement.set(anyInt(), any(), any(Class.class))).thenReturn(boundStatement);
        when(boundStatement.unset(anyInt())).thenReturn(boundStatement);

        updateCQLBeginning = "UPDATE " + targetKeyspaceTableName;

        StringBuilder sb = new StringBuilder();
        sb.append(" SET ")
                .append(targetCounterColumns.stream().map(column -> column + "=" + column + "+?")
                        .collect(Collectors.joining(",")))
                .append(" WHERE ").append(keyEqualsBindJoinedWithAND(targetPrimaryKey));
        counterUpdateCQLEnding = sb.toString();

    }

    @Test
    public void smoke_basicCQL_Counter() {
        assertEquals(updateCQLBeginning + counterUpdateCQLEnding, targetUpdateStatement.getCQL());
    }

    @Test
    public void smoke_basicCQL_Other() {
        commonSetup(false, false, false);
        StringBuilder sb = new StringBuilder();
        sb.append(updateCQLBeginning).append(" SET ")
                .append(targetValueColumns.stream().map(column -> column + "=?").collect(Collectors.joining(",")))
                .append(" WHERE ").append(keyEqualsBindJoinedWithAND(targetPrimaryKey));

        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        assertEquals(sb.toString(), targetUpdateStatement.getCQL());
    }

    @Test
    public void smoke_basicCQL_Constant() {
        commonSetup(false, true, false);
        StringBuilder sb = new StringBuilder();
        sb.append(updateCQLBeginning).append(" SET ")
                .append(targetValueColumns.stream().map(column -> column + "=?").collect(Collectors.joining(",")))
                .append(",")
                .append(IntStream.range(0, constantColumns.size())
                        .mapToObj(i -> constantColumns.get(i) + "=" + constantColumnValues.get(i))
                        .collect(Collectors.joining(",")))
                .append(" WHERE ").append(keyEqualsBindJoinedWithAND(targetPrimaryKey));

        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        assertEquals(sb.toString(), targetUpdateStatement.getCQL());
    }

    @Test
    public void cql_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        assertEquals(updateCQLBeginning +" USING TTL ?"+ counterUpdateCQLEnding, targetUpdateStatement.getCQL());
    }

    @Test
    public void cql_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        assertEquals(updateCQLBeginning +" USING TIMESTAMP ?"+ counterUpdateCQLEnding, targetUpdateStatement.getCQL());
    }

    @Test
    public void cql_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        assertEquals(updateCQLBeginning +" USING TTL ? AND TIMESTAMP ?"+ counterUpdateCQLEnding, targetUpdateStatement.getCQL());
    }

    @Test
    public void bind_withStandardInput() {
        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);

        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, 3600,null,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);

        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null,10000L,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);

        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, 3600,10000L,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+2)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void testBindOriginRowNull() {
        assertThrows(RuntimeException.class, () -> targetUpdateStatement.bind(null, targetRow, 30, 123456789L,
                getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType)));
    }

    @Test
    public void bind_nonCounter_withStandardInput() {
        commonSetup(false, false, false);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_explodeMap_withStandardInput() {
        commonSetup(true, false, false);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);
        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null, null,
                getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_nonCounter_withExtraColumn() {
        commonSetup(false, false, false);
        targetColumnNames.add("extraColumn");
        targetColumnTypes.add(DataTypes.TEXT);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);

        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() - 1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withExceptionWhenBindingValue() {
        when(targetTable.getCorrespondingIndex(anyInt())).thenReturn(0);
        when(originTable.getAndConvertData(anyInt(), eq(originRow))).thenThrow(new RuntimeException("Error binding value"));

        assertThrows(RuntimeException.class, () -> targetUpdateStatement.bind(originRow, targetRow, null,null,null,null));
    }

    @Test
    public void bind_nullToUnset_true() {
        // Simulate a null value from origin
        when(originTable.getAndConvertData(anyInt(), eq(originRow))).thenReturn(null);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetUpdateStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
    }

}
