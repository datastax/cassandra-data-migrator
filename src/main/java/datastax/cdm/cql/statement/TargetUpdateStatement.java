package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.PKFactory;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
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
        setColumnIndexesToBind();

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
        EnhancedPK pk = pkFactory.getTargetPK(originRow);
        boundStatement = pkFactory.bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, currentBindIndex);

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

                targetUpdateCQL.append(key).append("=");
                if (constantColumnNames.contains(key))
                    targetUpdateCQL.append(constantColumnValues.get(constantColumnNames.indexOf(key)));
                else {
                    if (usingCounter && counterIndexes.contains(currentColumn))
                        targetUpdateCQL.append(key).append("+?");
                    else
                        targetUpdateCQL.append("?");

                    bindIndex++;
                }
            }
            currentColumn++;
        }

        targetUpdateCQL.append(" WHERE ").append(pkFactory.getWhereClause(PKFactory.Side.TARGET));
        return targetUpdateCQL.toString();
    }

    private void setColumnIndexesToBind() {
        int currentColumn = 0;
        for (String key : targetColumnNames) {
            if (!ColumnsKeysTypes.getTargetPKNames(propertyHelper).contains(key)) {
                columnIndexesToBind.add(currentColumn);
            }
            currentColumn++;
        }
    }

}
