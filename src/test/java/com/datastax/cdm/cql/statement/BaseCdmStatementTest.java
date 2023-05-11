package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class BaseCdmStatementTest {

    @Mock
    EnhancedSession session;

    @Mock
    CqlSession cqlSession;

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    CqlTable cqlTable;

    @Mock
    PreparedStatement preparedStatement;

    BaseCdmStatement baseCdmStatement;

    String statement = "SELECT * FROM keyspace_name.table_name";

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        when(session.getCqlTable()).thenReturn(cqlTable);
        when(session.getCqlSession()).thenReturn(cqlSession);
        when(cqlSession.prepare(statement)).thenReturn(preparedStatement);

        baseCdmStatement = new BaseCdmStatement(propertyHelper, session);
        baseCdmStatement.statement = statement;
    }

    @Test
    public void smoke_basicCQL() {
        baseCdmStatement = new BaseCdmStatement(propertyHelper, session);
        baseCdmStatement.statement = statement;
        assertEquals(statement, baseCdmStatement.getCQL());
    }

    @Test
    public void testConstructor_success() {
        assertNotNull(baseCdmStatement);
    }

    @Test
    public void testConstructor_nullPropertyHelper() {
        assertThrows(RuntimeException.class, () -> new BaseCdmStatement(null, session));
    }

    @Test
    public void testConstructor_nullSession() {
        assertThrows(RuntimeException.class, () -> new BaseCdmStatement(propertyHelper, null));
    }

    @Test
    public void testConstructor_nullCqlTable() {
        when(session.getCqlTable()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> new BaseCdmStatement(propertyHelper, session));
    }

    @Test
    public void testPrepareStatement_success() {
        PreparedStatement result = baseCdmStatement.prepareStatement();
        assertEquals(preparedStatement, result);
    }

    @Test
    public void testPrepareStatement_nullCqlSession() {
        when(session.getCqlSession()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> baseCdmStatement.prepareStatement());
    }

    @Test
    public void testPrepareStatement_emptyStatement() {
        baseCdmStatement.statement = "";
        assertThrows(RuntimeException.class, () -> baseCdmStatement.prepareStatement());
    }
}
