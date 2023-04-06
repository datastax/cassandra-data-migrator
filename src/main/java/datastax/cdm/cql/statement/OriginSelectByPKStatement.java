package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.data.PKFactory;
import datastax.cdm.data.Record;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class OriginSelectByPKStatement extends AbstractOriginSelectStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<MigrateDataType> originPKTypes;

    public OriginSelectByPKStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
        originPKTypes = cqlHelper.getPKFactory().getPKTypes(PKFactory.Side.ORIGIN);
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
        if (pk.isError() || pk.getPKValues().size() != originPKTypes.size())
            throw new RuntimeException("PK is in Error state, or the number of values does not match the number of bind types");

        BoundStatement boundStatement = prepareStatement().bind();
        for (int i = 0; i< originPKTypes.size(); i++) {
            if (originPKTypes.get(i).getTypeClass() != pk.getPKValues().get(i).getClass())
                throw new RuntimeException("PK value at index " + i + " does not match the expected type");

            boundStatement = boundStatement.set(i,pk.getPKValues().get(i), originPKTypes.get(i).getTypeClass());
        }

        return boundStatement
            .setConsistencyLevel(cqlHelper.getReadConsistencyLevel())
            .setPageSize(cqlHelper.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        return cqlHelper.getPKFactory().getWhereClause(PKFactory.Side.ORIGIN);
    }

}
