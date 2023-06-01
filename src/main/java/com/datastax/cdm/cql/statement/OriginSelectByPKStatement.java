package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OriginSelectByPKStatement extends OriginSelectStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public OriginSelectByPKStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
    }

    public Record getRecord(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null == boundStatement)
            return null;

        ResultSet resultSet = session.getCqlSession().execute(boundStatement);
        if (null == resultSet)
            return null;

        Row row = resultSet.one();
        if (null == row)
            return null;

        return new Record(pk, row, null);
    }

    @Override
    public BoundStatement bind(Object... binds) {
        if (null == binds
                || binds.length != 1
                || null == binds[0]
                || !(binds[0] instanceof EnhancedPK))
            throw new RuntimeException("Expected 1 nullable bind of type EnhancedPK, got " + binds.length);

        EnhancedPK pk = (EnhancedPK) binds[0];

        BoundStatement boundStatement = prepareStatement().bind();
        boundStatement = session.getPKFactory().bindWhereClause(PKFactory.Side.ORIGIN, pk, boundStatement, 0);

        return boundStatement
                .setConsistencyLevel(cqlTable.getReadConsistencyLevel())
                .setPageSize(cqlTable.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        return session.getPKFactory().getWhereClause(PKFactory.Side.ORIGIN);
    }

}
