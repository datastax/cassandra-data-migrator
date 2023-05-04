package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.*;

import java.util.ArrayList;
import java.util.List;

public class BaseCdmStatement {

    protected IPropertyHelper propertyHelper;
    protected CqlTable cqlTable;
    protected String statement = "";
    protected EnhancedSession session;

    protected List<String> resultColumns = new ArrayList<>();

    public BaseCdmStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        if (null==propertyHelper || null==session || null==session.getCqlTable())
            throw new RuntimeException("PropertyHelper or EnhancedSession or EnhancedSession.getCqlTable() is not set");
        this.propertyHelper = propertyHelper;
        this.cqlTable = session.getCqlTable();
        this.session = session;
    }

    public PreparedStatement prepareStatement() {
        if (null==session || null==session.getCqlSession())
            throw new RuntimeException("Session is not set");
        if (null == statement || statement.isEmpty())
            throw new RuntimeException("Statement is not set");
        return session.getCqlSession().prepare(statement);
    }

    public String getCQL() {
        return statement;
    }

}