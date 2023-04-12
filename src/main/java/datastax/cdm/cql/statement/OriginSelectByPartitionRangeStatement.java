package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.feature.Featureset;
import datastax.cdm.feature.OriginFilterCondition;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class OriginSelectByPartitionRangeStatement extends AbstractOriginSelectStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Long defaultMinPartition;
    private final Long defaultMaxPartition;

    public OriginSelectByPartitionRangeStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        defaultMinPartition = propertyHelper.getLong(KnownProperties.PARTITION_MIN);
        defaultMaxPartition = propertyHelper.getLong(KnownProperties.PARTITION_MAX);
    }

    @Override
    public BoundStatement bind(Object... binds) {
        if (null==binds
                || binds.length != 2
                || !(null==binds[0] || binds[0] instanceof BigInteger)
                || !(null==binds[1] || binds[1] instanceof BigInteger))
            throw new RuntimeException("Expected 2 nullable bind of type BigInteger, got " + binds.length);

        BigInteger min = (null==binds[0]) ? BigInteger.valueOf(defaultMinPartition) : (BigInteger) binds[0];
        BigInteger max = (null==binds[1]) ? BigInteger.valueOf(defaultMaxPartition) : (BigInteger) binds[1];

        PreparedStatement preparedStatement = prepareStatement();
        return preparedStatement.bind(
                    cqlHelper.hasRandomPartitioner() ? min : min.longValueExact(),
                    cqlHelper.hasRandomPartitioner() ? max : max.longValueExact())
                .setConsistencyLevel(cqlHelper.getReadConsistencyLevel())
                .setPageSize(cqlHelper.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        String partitionKey = PropertyHelper.asString(ColumnsKeysTypes.getOriginPartitionKeyNames(propertyHelper), KnownProperties.PropertyType.STRING_LIST).trim();
        return "TOKEN(" + partitionKey + ") >= ? AND TOKEN(" + partitionKey + ") <= ?";
    }

    @Override
    protected String buildStatement() {
        StringBuilder sb = new StringBuilder(super.buildStatement());
        sb.append(cqlHelper.getFeature(Featureset.ORIGIN_FILTER).getAsString(OriginFilterCondition.Property.CONDITION));
        sb.append(" ALLOW FILTERING");
        return sb.toString();
    }
}
