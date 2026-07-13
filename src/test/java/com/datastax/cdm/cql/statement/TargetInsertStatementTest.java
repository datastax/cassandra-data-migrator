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

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class TargetInsertStatementTest extends CommonMocks {

    TargetInsertStatement targetInsertStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);
        // Ensure prepareStatement().bind() returns the boundStatement mock
        when(targetInsertStatement.prepareStatement()).thenReturn(preparedStatement);
        when(preparedStatement.bind()).thenReturn(boundStatement);
        // Chain set and unset to return boundStatement
        when(boundStatement.set(anyInt(), any(), any(Class.class))).thenReturn(boundStatement);
        when(boundStatement.unset(anyInt())).thenReturn(boundStatement);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(targetKeyspaceTableName).append(" (")
                .append(String.join(",", targetColumnNames)).append(")").append(" VALUES (")
                .append(String.join(",", Collections.nCopies(targetColumnNames.size(), "?"))).append(")");
        String insertStatement = sb.toString();

        assertEquals(insertStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName + " (" + String.join(",", targetColumnNames)
                + ")" + " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")"
                + " USING TTL ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName + " (" + String.join(",", targetColumnNames)
                + ")" + " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")"
                + " USING TIMESTAMP ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName + " (" + String.join(",", targetColumnNames)
                + ")" + " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")"
                + " USING TTL ? AND TIMESTAMP ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_ConstantColumns() {
        commonSetup(false, true, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(targetKeyspaceTableName).append(" (")
                .append(String.join(",", targetColumnNames)).append(")").append(" VALUES (")
                .append(String.join(",", Collections.nCopies(targetColumnNames.size() - constantColumns.size(), "?")))
                .append(",").append(String.join(",", constantColumnValues)).append(")");
        String insertStatement = sb.toString();

        assertEquals(insertStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void bind_withStandardInput() {
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, 3600, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() + 1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, 10000L, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() + 1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, 3600, 10000L, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() + 2)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withExplodeMap() {
        commonSetup(true, false, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null,
                getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withConstantColumns() {
        commonSetup(false, true, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null,
                getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() - constantColumns.size())).set(anyInt(), any(),
                any(Class.class));
    }

    @Test
    public void bind_extraTargetColumn() {
        targetColumnNames.add("extraColumn");
        targetColumnTypes.add(DataTypes.TEXT);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null,
                getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size() - 1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withNullOriginRow() {
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> targetInsertStatement.bind(null, targetRow, 3600, 123456789L, explodeMapKey, explodeMapValue));
        assertEquals("Origin row is null", exception.getMessage());
    }

    @Test
    public void bind_withUsingCounterTrue() {
        when(targetTable.getCounterIndexes()).thenReturn(Collections.singletonList(0));
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(originRow,
                targetRow, 3600, 123456789L, explodeMapKey, explodeMapValue));
        assertEquals("Cannot INSERT onto a counter table, use UPDATE instead", exception.getMessage());
    }

    @Test
    public void bind_withExceptionWhenBindingValue() {
        when(targetTable.getCorrespondingIndex(anyInt())).thenReturn(0);
        when(originTable.getAndConvertData(anyInt(), eq(originRow)))
                .thenThrow(new RuntimeException("Error binding value"));

        assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(originRow, targetRow, 3600, 123456789L,
                explodeMapKey, explodeMapValue));
    }

    @Test
    public void bind_withVectorColumns() {
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);
        assertTrue(targetInsertStatement.targetColumnNames.contains(vectorCol));
        assertTrue(6 == targetInsertStatement.targetColumnNames.size());
        assertEquals(vectorColType, targetInsertStatement.targetColumnTypes.get(5));
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_nullToUnset_true() {
        // Simulate a null value from origin
        when(originTable.getAndConvertData(anyInt(), eq(originRow))).thenReturn(null);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
    }

    @Test
    public void bind_withNullValue_shouldCallUnset() {
        // Setup: return null for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(null);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).unset(anyInt());
    }

    @Test
    public void bind_withEmptyList_shouldCallUnset() {
        // Setup: return empty list for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(Collections.emptyList());
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).unset(anyInt());
    }

    @Test
    public void bind_withEmptySet_shouldCallUnset() {
        // Setup: return empty set for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(Collections.emptySet());
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).unset(anyInt());
    }

    @Test
    public void bind_withEmptyMap_shouldCallUnset() {
        // Setup: return empty map for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(Collections.emptyMap());
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).unset(anyInt());
    }

    @Test
    public void bind_withNonEmptyList_shouldCallSet() {
        // Setup: return non-empty list for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(Arrays.asList("a", "b"));
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withNonEmptySet_shouldCallSet() {
        // Setup: return non-empty set for one column
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(Collections.singleton("value"));
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withNonEmptyMap_shouldCallSet() {
        // Setup: return non-empty map for one column
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenReturn(map);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        // Act
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Assert
        assertNotNull(result);
        verify(boundStatement, atLeastOnce()).set(anyInt(), any(), any(Class.class));
    }

    // ---- bind(): input validation and bind-index ordering ----

    /**
     * When TTL columns are enabled but no TTL value is supplied, bind() rejects the input with a RuntimeException
     * naming the missing TTL property.
     */
    @Test
    public void bind_checkBindInputsThrowsWhenTtlMissing() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> targetInsertStatement.bind(originRow, targetRow, null, null, null, null));
        assertEquals(KnownProperties.ORIGIN_TTL_NAMES + " specified, but no TTL value was provided", ex.getMessage());
    }

    /**
     * When writetime columns are enabled but no writetime value is supplied, bind() rejects the input with a
     * RuntimeException naming the missing writetime property.
     */
    @Test
    public void bind_checkBindInputsThrowsWhenWriteTimeMissing() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> targetInsertStatement.bind(originRow, targetRow, null, null, null, null));
        assertEquals(KnownProperties.ORIGIN_WRITETIME_NAMES + " specified, but no WriteTime value was provided",
                ex.getMessage());
    }

    /**
     * bind() binds every target column exactly once at sequential indices 0, 1, 2, ...
     */
    @Test
    public void bind_bindsAllColumnsAtSequentialIndices() {
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null, null, null);
        assertNotNull(result);

        ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(boundStatement, times(targetColumnNames.size())).set(indexCaptor.capture(), any(), any(Class.class));

        List<Integer> capturedIndices = indexCaptor.getAllValues();
        for (int i = 0; i < capturedIndices.size(); i++) {
            assertEquals(i, capturedIndices.get(i).intValue());
        }
    }

    /**
     * Constant columns are not bound: bind() sets only the non-constant columns, at sequential indices starting from
     * zero.
     */
    @Test
    public void bind_skipsConstantColumns() {
        commonSetup(false, true, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);

        targetInsertStatement.bind(originRow, targetRow, null, null, null, null);

        // Constant columns are skipped; only the remaining columns are bound.
        int expectedBindCount = targetColumnNames.size() - constantColumns.size();
        verify(boundStatement, times(expectedBindCount)).set(indexCaptor.capture(), any(), any(Class.class));

        List<Integer> capturedIndices = indexCaptor.getAllValues();
        for (int i = 0; i < capturedIndices.size(); i++) {
            assertEquals(i, capturedIndices.get(i).intValue());
        }
    }

    /**
     * With an explode-map column present, bind() sets every target column at sequential indices starting from zero.
     */
    @Test
    public void bind_bindsExplodeMapColumnsAtSequentialIndices() {
        commonSetup(true, false, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);

        targetInsertStatement.bind(originRow, targetRow, null, null, getSampleData(explodeMapKeyType),
                getSampleData(explodeMapValueType));

        verify(boundStatement, times(targetColumnNames.size())).set(indexCaptor.capture(), any(), any(Class.class));
        List<Integer> capturedIndices = indexCaptor.getAllValues();
        for (int i = 0; i < capturedIndices.size(); i++) {
            assertEquals(i, capturedIndices.get(i).intValue());
        }
    }

    /**
     * With both TTL and writetime enabled, bind() binds each target column at sequential indices and then appends the
     * TTL value (as an Integer) and the writetime value (as a Long) at the two trailing indices, in that order.
     */
    @Test
    public void bind_appendsTtlAndWritetimeAtTrailingIndicesWithTypes() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
        ArgumentCaptor<Class> classCaptor = ArgumentCaptor.forClass(Class.class);

        Integer ttl = 7200;
        Long writeTime = 42L;

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, ttl, writeTime, null, null);
        assertNotNull(result);

        verify(boundStatement, times(targetColumnNames.size() + 2)).set(indexCaptor.capture(), valueCaptor.capture(),
                classCaptor.capture());

        List<Integer> allIndices = indexCaptor.getAllValues();
        List<Object> allValues = valueCaptor.getAllValues();
        List<Class> allClasses = classCaptor.getAllValues();

        // Columns are bound at sequential indices.
        for (int i = 0; i < targetColumnNames.size(); i++) {
            assertEquals(i, allIndices.get(i).intValue());
        }

        // TTL is bound immediately after the columns, with its value and Integer type.
        int ttlIdx = targetColumnNames.size();
        assertEquals(ttlIdx, allIndices.get(ttlIdx).intValue());
        assertEquals(ttl, allValues.get(ttlIdx));
        assertEquals(Integer.class, allClasses.get(ttlIdx));

        // WriteTime is bound at the next index, with its value and Long type.
        int wtIdx = targetColumnNames.size() + 1;
        assertEquals(wtIdx, allIndices.get(wtIdx).intValue());
        assertEquals(writeTime, allValues.get(wtIdx));
        assertEquals(Long.class, allClasses.get(wtIdx));
    }

    /**
     * When converting an origin value throws, bind() wraps the failure in a RuntimeException whose message identifies
     * the value being bound.
     */
    @Test
    public void bind_exceptionWithNonNullOriginValue() {
        when(targetTable.getCorrespondingIndex(0)).thenReturn(0);
        when(originTable.getAndConvertData(0, originRow)).thenThrow(new RuntimeException("binding error"));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(originRow,
                targetRow, 3600, 123456789L, getSampleData(explodeMapKeyType), getSampleData(explodeMapValueType)));

        assertTrue(ex.getMessage().contains("Error trying to bind value"));
    }

}
