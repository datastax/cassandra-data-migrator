package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.feature.Feature;
import datastax.cdm.feature.FeatureFactory;
import datastax.cdm.feature.Featureset;
import datastax.cdm.feature.WritetimeTTLColumn;
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

    public TargetUpdateStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper, CqlSession session) {
        super(propertyHelper, cqlHelper, session);
        this.columnIndexesToBind = new ArrayList<>();
        setColumnIndexesToBind();

        Feature feature = cqlHelper.getFeature(Featureset.WRITETIME_TTL_COLUMN);
        if (FeatureFactory.isEnabled(feature) &&
                null != feature.getNumberList(WritetimeTTLColumn.Property.TTL_INDEXES))
            usingTTL = true;
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

        for (int targetIndex : columnIndexesToBind) {
            MigrateDataType targetDataType = targetColumnTypes.get(targetIndex);
            MigrateDataType originDataType = null;
            Integer originIndex = ColumnsKeysTypes.getTargetToOriginColumnIndexes(propertyHelper).get(targetIndex);
            Object bindValue;

            if(usingCounter && counterIndexes.contains(targetIndex)) {
                bindValue = (originRow.getLong(originIndex) - (null==targetRow ? 0 : targetRow.getLong(targetIndex)));
                originDataType = targetDataType;
            }
            else if (targetIndex==explodeMapKeyIndex) {
                bindValue = explodeMapKey;
                originDataType = explodeMapKeyDataType;
            }
            else if (targetIndex==explodeMapValueIndex) {
                bindValue = explodeMapValue;
                originDataType = explodeMapValueDataType;
            }
            else if (originIndex < 0) {
                bindValue = null;
            }
            else {
                originDataType = originColumnTypes.get(originIndex);
                Object originValue = cqlHelper.getData(originDataType, originIndex, originRow);
                if (targetDataType.hasUDT() && udtMappingEnabled) {
                    bindValue = udtMapper.convert(true, targetIndex, originValue);
                }
                else {
                    bindValue = originValue;
                }

            }
            if (null != bindValue) {
                bindValue = (targetDataType.equals(originDataType)) ? bindValue : MigrateDataType.convert(bindValue, originDataType, targetDataType, cqlHelper.getCodecRegistry());
            }
            boundStatement = boundStatement.set(currentBindIndex++, bindValue, targetDataType.getTypeClass());
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
        targetUpdateCQL.append(ColumnsKeysTypes.getTargetKeyspaceTable(propertyHelper));
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
