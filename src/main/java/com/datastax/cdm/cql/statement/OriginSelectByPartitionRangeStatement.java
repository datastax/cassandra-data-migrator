package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.OriginFilterCondition;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public class OriginSelectByPartitionRangeStatement extends OriginSelectStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Long defaultMinPartition;
    private final Long defaultMaxPartition;

    public OriginSelectByPartitionRangeStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);

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
        // random partitioner uses BigInteger, the normal partitioner uses long
        return preparedStatement.bind(
                    cqlTable.hasRandomPartitioner() ? min : min.longValueExact(),
                    cqlTable.hasRandomPartitioner() ? max : max.longValueExact())
                .setConsistencyLevel(cqlTable.getReadConsistencyLevel())
                .setPageSize(cqlTable.getFetchSizeInRows());
    }

    @Override
    protected String whereBinds() {
        String partitionKey = PropertyHelper.asString(cqlTable.getPartitionKeyNames(true), KnownProperties.PropertyType.STRING_LIST).trim();
        return "TOKEN(" + partitionKey + ") >= ? AND TOKEN(" + partitionKey + ") <= ?";
    }

    @Override
    protected String buildStatement() {
        StringBuilder sb = new StringBuilder(super.buildStatement());
        sb.append(((OriginFilterCondition) cqlTable.getFeature(Featureset.ORIGIN_FILTER)).getFilterCondition());
        sb.append(" ALLOW FILTERING");
        return sb.toString();
    }
}
