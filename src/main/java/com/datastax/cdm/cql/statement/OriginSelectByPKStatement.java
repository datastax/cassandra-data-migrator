package com.datastax.cdm.cql.statement;

import com.datastax.cdm.properties.ColumnsKeysTypes;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.cdm.job.MigrateDataType;
import com.datastax.cdm.cql.CqlHelper;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class OriginSelectByPKStatement extends AbstractOriginSelectStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<MigrateDataType> originPKTypes;

    public OriginSelectByPKStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper, CqlSession session) {
        super(propertyHelper, cqlHelper, session);
        originPKTypes = ColumnsKeysTypes.getOriginPKTypes(propertyHelper);
    }

    public Record getRecord(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null==boundStatement)
            return null;

        ResultSet resultSet = session.execute(boundStatement);
        if (null==resultSet)
            return null;

        Row row = resultSet.one();
        if (null==row)
            return null;

        return new Record(pk, row, null);
    }

    @Override
    public BoundStatement bind(Object... binds) {
        if (null==binds
                || binds.length != 1
                || null==binds[0]
                || !(binds[0] instanceof EnhancedPK))
            throw new RuntimeException("Expected 1 nullable bind of type EnhancedPK, got " + binds.length);

        EnhancedPK pk = (EnhancedPK) binds[0];

        BoundStatement boundStatement = prepareStatement().bind();
        boundStatement = cqlHelper.getPKFactory().bindWhereClause(PKFactory.Side.ORIGIN, pk, boundStatement, 0);

        return boundStatement
            .setConsistencyLevel(cqlHelper.getReadConsistencyLevel())
            .setPageSize(cqlHelper.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        return cqlHelper.getPKFactory().getWhereClause(PKFactory.Side.ORIGIN);
    }

}
