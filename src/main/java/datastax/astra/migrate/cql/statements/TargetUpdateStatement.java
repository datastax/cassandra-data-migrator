package datastax.astra.migrate.cql.statements;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.cql.PKFactory;
import datastax.astra.migrate.cql.features.ExplodeMap;
import datastax.astra.migrate.cql.features.FeatureFactory;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TargetUpdateStatement extends AbstractTargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<Integer> columnIndexesToBind;
    private boolean usingTTL;

    public TargetUpdateStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
        this.columnIndexesToBind = new ArrayList<>();
        setExplodeMapColumnsAndColumnIndexesToBind();

        List<String> ttlColumnNames = propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_INDEXES);
        if (null != ttlColumnNames && !ttlColumnNames.isEmpty()) usingTTL = true;
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
        if (null == originRow)
            throw new RuntimeException("Origin row is null");
        checkBindInputs(ttl, writeTime, explodeMapKey, explodeMapValue);

        BoundStatement boundStatement = prepareStatement().bind();

        int currentBindIndex = 0;
        if (usingTTL) {
            boundStatement = boundStatement.set(currentBindIndex++, ttl, Integer.class);
        }
        if (usingWriteTime) {
            boundStatement = boundStatement.set(currentBindIndex++, writeTime, Long.class);
        }

        for (int index : columnIndexesToBind) {
            MigrateDataType dataType = targetColumnTypes.get(index);
            Object bindValue;

            if(usingCounter && counterIndexes.contains(index)) bindValue = (originRow.getLong(index) - (null==targetRow ? 0 : targetRow.getLong(index)));
            else if (index==explodeMapKeyIndex) bindValue = explodeMapKey;
            else if (index==explodeMapValueIndex) bindValue = explodeMapValue;
            else bindValue = cqlHelper.getData(dataType, index, originRow);

            boundStatement = boundStatement.set(currentBindIndex++, bindValue, dataType.getTypeClass());
        }

        PKFactory pkFactory = cqlHelper.getPKFactory();
        for (int index : pkFactory.getPKIndexesToBind(PKFactory.Side.TARGET)) {
            MigrateDataType dataType = targetColumnTypes.get(index);
            Object bindValue = cqlHelper.getData(dataType, index, originRow);
            boundStatement = boundStatement.set(currentBindIndex++, bindValue, dataType.getTypeClass());
        }

        return boundStatement
                .setConsistencyLevel(cqlHelper.getWriteConsistencyLevel())
                .setTimeout(Duration.ofSeconds(10));
    }

    @Override
    protected String buildStatement() {
        PKFactory pkFactory = cqlHelper.getPKFactory();
        StringBuilder targetUpdateCQL = new StringBuilder("UPDATE ");
        targetUpdateCQL.append(propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE));
        targetUpdateCQL.append(usingTTLTimestamp());
        targetUpdateCQL.append(" SET ");
        int currentColumn = 0;
        for (String key : targetColumnNames) {
            if (!pkFactory.getPKNames(PKFactory.Side.TARGET).contains(key)) {
                if (bindIndex > 0)
                    targetUpdateCQL.append(",");
                if (counterIndexes.contains(currentColumn))
                    targetUpdateCQL.append(key).append("=").append(key).append("+?");
                else
                    targetUpdateCQL.append(key).append("=?");

                bindIndex++;
            }
            currentColumn++;
        }

        for (int i=0; i<constantColumnNames.size(); i++) {
            if (!pkFactory.getPKNames(PKFactory.Side.TARGET).contains(constantColumnNames.get(i)))
                targetUpdateCQL.append(",").append(constantColumnNames.get(i)).append("=").append(constantColumnValues.get(i));
        }

        targetUpdateCQL.append(" WHERE ").append(pkFactory.getWhereClause(PKFactory.Side.TARGET));
        return targetUpdateCQL.toString();
    }

    private void setExplodeMapColumnsAndColumnIndexesToBind() {
        PKFactory pkFactory = cqlHelper.getPKFactory();
        int currentColumn = 0;
        for (String key : targetColumnNames) {
            if (FeatureFactory.isEnabled(explodeMapFeature)) {
                if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME)))
                    explodeMapKeyIndex = currentColumn;
                else if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.VALUE_COLUMN_NAME)))
                    explodeMapValueIndex = currentColumn;
            }

            if (!pkFactory.getPKNames(PKFactory.Side.TARGET).contains(key)) {
                columnIndexesToBind.add(currentColumn);
            }
            currentColumn++;
        }
    }

}
