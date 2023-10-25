package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TargetInsertStatementTest extends CommonMocks {

    TargetInsertStatement targetInsertStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(targetKeyspaceTableName)
                .append(" (").append(String.join(",",targetColumnNames)).append(")")
                .append(" VALUES (").append(String.join(",",Collections.nCopies(targetColumnNames.size(),"?"))).append(")");
        String insertStatement = sb.toString();

        assertEquals(insertStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName +
                " (" + String.join(",", targetColumnNames) + ")" +
                " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")" +
                " USING TTL ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName +
                " (" + String.join(",", targetColumnNames) + ")" +
                " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")" +
                " USING TIMESTAMP ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        String expectedStatement = "INSERT INTO " + targetKeyspaceTableName +
                " (" + String.join(",", targetColumnNames) + ")" +
                " VALUES (" + String.join(",", Collections.nCopies(targetColumnNames.size(), "?")) + ")" +
                " USING TTL ? AND TIMESTAMP ?";

        assertEquals(expectedStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void cql_ConstantColumns() {
        commonSetup(false,true,false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(targetKeyspaceTableName)
                .append(" (")
                .append(String.join(",",targetColumnNames))
                .append(")")
                .append(" VALUES (").append(String.join(",",Collections.nCopies(targetColumnNames.size()-constantColumns.size(),"?")))
                .append(",").append(String.join(",",constantColumnValues))
                .append(")");
        String insertStatement = sb.toString();

        assertEquals(insertStatement, targetInsertStatement.getCQL());
    }

    @Test
    public void bind_withStandardInput() {
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null,null,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTL() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, 3600,null,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null,10000L,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+1)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withTTLAndWritetime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, 3600,10000L,null,null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()+2)).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withExplodeMap() {
        commonSetup(true,false,false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null,null,getSampleData(explodeMapKeyType),getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_withConstantColumns() {
        commonSetup(false,true, false);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null,null,getSampleData(explodeMapKeyType),getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()-constantColumns.size())).set(anyInt(), any(), any(Class.class));
    }

    @Test
    public void bind_extraTargetColumn() {
        targetColumnNames.add("extraColumn");
        targetColumnTypes.add(DataTypes.TEXT);
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null,null,getSampleData(explodeMapKeyType),getSampleData(explodeMapValueType));
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size()-1)).set(anyInt(), any(), any(Class.class));
    }


    @Test
    public void bind_withNullOriginRow() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(null, targetRow, 3600, 123456789L, explodeMapKey, explodeMapValue));
        assertEquals("Origin row is null", exception.getMessage());
    }

    @Test
    public void bind_withUsingCounterTrue() {
        when(targetTable.getCounterIndexes()).thenReturn(Collections.singletonList(0));
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(originRow, targetRow, 3600, 123456789L, explodeMapKey, explodeMapValue));
        assertEquals("Cannot INSERT onto a counter table, use UPDATE instead", exception.getMessage());
    }

    @Test
    public void bind_withExceptionWhenBindingValue() {
        when(targetTable.getCorrespondingIndex(anyInt())).thenReturn(0);
        when(originTable.getAndConvertData(anyInt(), eq(originRow))).thenThrow(new RuntimeException("Error binding value"));

        assertThrows(RuntimeException.class, () -> targetInsertStatement.bind(originRow, targetRow, 3600, 123456789L, explodeMapKey, explodeMapValue));
    }


    @Test
    public void bind_withVectorColumns() {
        targetInsertStatement = new TargetInsertStatement(propertyHelper, targetSession);
        assertTrue(targetInsertStatement.targetColumnNames.contains(vectorCol));
        assertTrue(6 == targetInsertStatement.targetColumnNames.size());
        assertEquals(vectorColType, targetInsertStatement.targetColumnTypes.get(5));
        BoundStatement result = targetInsertStatement.bind(originRow, targetRow, null, null,null, null);
        assertNotNull(result);
        verify(boundStatement, times(targetColumnNames.size())).set(anyInt(), any(), any(Class.class));
    }

}
