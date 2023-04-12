package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.data.PKFactory;
import datastax.cdm.data.Record;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class TargetSelectByPKStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public TargetSelectByPKStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        this.session = cqlHelper.getTargetSession();

        this.statement = buildStatement();

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

        return new Record(pk, null, row);
    }

    public CompletionStage<AsyncResultSet> getAsyncResult(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null==boundStatement)
            return null;
        return session.executeAsync(boundStatement);
    }

    private BoundStatement bind(EnhancedPK pk) {
        BoundStatement boundStatement = prepareStatement().bind()
                .setConsistencyLevel(cqlHelper.getReadConsistencyLevel());

        boundStatement = cqlHelper.getPKFactory().bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, 0);
        return boundStatement;
    }

    private String buildStatement() {
        return "SELECT " + PropertyHelper.asString(ColumnsKeysTypes.getTargetColumnNames(propertyHelper), KnownProperties.PropertyType.STRING_LIST)
                + " FROM " + propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE)
                + " WHERE " + cqlHelper.getPKFactory().getWhereClause(PKFactory.Side.TARGET);
    }
}