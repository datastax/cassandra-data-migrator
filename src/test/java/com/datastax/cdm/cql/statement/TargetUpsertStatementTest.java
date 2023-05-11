package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.oss.driver.api.core.cql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TargetUpsertStatementTest extends CommonMocks {

    TargetUpsertStatement targetUpsertStatement;

    @BeforeEach
    public void setup() {
        commonSetup();
        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(targetKeyspaceTableName)
                .append(" (").append(String.join(",",targetColumnNames)).append(")")
                .append(" VALUES (").append(String.join(",",Collections.nCopies(targetColumnNames.size(),"?"))).append(")");
        String insertStatement = sb.toString();
        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession, insertStatement);

        assertEquals(insertStatement, targetUpsertStatement.getCQL());
    }

    @Test
    public void bindRecord_nullRecord_throwsRuntimeException() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.bindRecord(null));
        assertEquals("record is null", exception.getMessage());
    }

    @Test
    public void checkBindInputs_TTL_throwsRuntimeException() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, null, null));
        assertTrue(exception.getMessage().endsWith("but no TTL value was provided"));
    }

    @Test
    public void checkBindInputs_Writetime_throwsRuntimeException() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, null, null));
        assertTrue(exception.getMessage().endsWith("but no WriteTime value was provided"));
    }

    @Test
    public void checkBindInputs_ExplodeMap_nullKey_throwsRuntimeException() {
        commonSetup(true,false,false);
        Object mockValue = mock(Object.class);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, null, mockValue));
        assertTrue(exception.getMessage().startsWith("ExplodeMap is enabled, but no map key"));
    }

    @Test
    public void checkBindInputs_ExplodeMap_nullValue_throwsRuntimeException() {
        commonSetup(true,false,false);
        String goodKey = "abc";

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, goodKey, null));
        assertTrue(exception.getMessage().startsWith("ExplodeMap is enabled, but no map value"));
    }

    @Test
    public void checkBindInputs_ExplodeMap_invalidKeyType_throwsRuntimeException() {
        commonSetup(true,false,false);
        Integer badKey = 1;

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, badKey, null));
        assertTrue(exception.getMessage().startsWith("ExplodeMap is enabled, but the map key type provided"));
    }

    @Test
    public void checkBindInputs_ExplodeMap_invalidValueType_throwsRuntimeException() {
        commonSetup(true,false,false);
        String goodKey = "abc";
        Integer badValue = 1;

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> targetUpsertStatement.checkBindInputs(null, null, goodKey, badValue));
        assertTrue(exception.getMessage().startsWith("ExplodeMap is enabled, but the map value type provided"));
    }

    @Test
    public void putRecord_executesBoundStatement() {
        targetUpsertStatement.putRecord(record);
        verify(targetCqlSession).execute(any(BoundStatement.class));
    }

    @Test
    public void executeAsync_executesAsyncStatement() {
        SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM keyspace_name.table_name");
        targetUpsertStatement.executeAsync(statement);
        verify(targetCqlSession).executeAsync(statement);
    }

    @Test
    public void constantColumns_goodConfig() {
        commonSetup(false,true,false);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);

        assertAll(
                () -> assertEquals(constantColumns,targetUpsertStatement.constantColumnNames),
                () -> assertEquals(constantColumnValues,targetUpsertStatement.constantColumnValues)
        );
    }

    @Test
    public void constantColumns_badConfig() {
        commonSetup(false,true,false);
        when(constantColumnsFeature.getValues()).thenReturn(Collections.singletonList("123"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> new TestTargetUpsertStatement(propertyHelper, targetSession));
        assertEquals("Constant column names and values are not the same size.", exception.getMessage());
    }

    @Test
    public void usingTTLTimestamp_NothingEnabled() {
        assertEquals("", targetUpsertStatement.usingTTLTimestamp());
    }

    @Test
    public void usingTTLTimestamp_TTLAndWriteTime() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        assertEquals(" USING TTL ? AND TIMESTAMP ?", targetUpsertStatement.usingTTLTimestamp());
    }

    @Test
    public void usingTTLTimestamp_TTLOnly() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(true);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(false);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        assertEquals(" USING TTL ?", targetUpsertStatement.usingTTLTimestamp());
    }

    @Test
    public void usingTTLTimestamp_WriteTimeOnly() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasTTLColumns()).thenReturn(false);
        when(writetimeTTLFeature.hasWritetimeColumns()).thenReturn(true);

        targetUpsertStatement = new TestTargetUpsertStatement(propertyHelper, targetSession);
        assertEquals(" USING TIMESTAMP ?", targetUpsertStatement.usingTTLTimestamp());
    }

    protected class TestTargetUpsertStatement extends TargetUpsertStatement {
        public TestTargetUpsertStatement(IPropertyHelper h, EnhancedSession s, String statement) {
            super(h, s);
            this.statement = statement;
        }
        public TestTargetUpsertStatement(IPropertyHelper h, EnhancedSession s) {
            this(h,s,"some arbitrary text");
        }

        @Override
        protected String buildStatement() { return statement; };

        @Override
        protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
            checkBindInputs(ttl, writeTime, explodeMapKey, explodeMapValue);
            return boundStatement;
        }
    }
}
