package datastax.astra.migrate.cql.statements;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.cql.EnhancedPK;
import datastax.astra.migrate.cql.PKFactory;
import datastax.astra.migrate.cql.Record;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class TargetSelectByPKStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<MigrateDataType> bindTypes;
    private final List<Integer> bindIndexes;

    public TargetSelectByPKStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        this.session = cqlHelper.getTargetSession();
        this.bindTypes = cqlHelper.getPKFactory().getPKTypes(PKFactory.Side.TARGET);
        this.bindIndexes = cqlHelper.getPKFactory().getPKIndexesToBind(PKFactory.Side.TARGET);

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
        int bindIndex = 0;
        for (Integer pkIndexToBind : bindIndexes) {
            MigrateDataType type = bindTypes.get(pkIndexToBind);
            Object value = pk.getPKValues().get(pkIndexToBind);
            boundStatement = boundStatement.set(bindIndex++, value, type.getTypeClass());
        }
        return boundStatement;
    }

    private String buildStatement() {
        return "SELECT " + propertyHelper.getAsString(KnownProperties.TARGET_COLUMN_NAMES)
                + " FROM " + propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE)
                + " WHERE " + cqlHelper.getPKFactory().getWhereClause(PKFactory.Side.TARGET);
    }
}