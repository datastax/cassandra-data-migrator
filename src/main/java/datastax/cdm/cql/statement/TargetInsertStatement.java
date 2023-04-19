package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.feature.ConstantColumns;
import datastax.cdm.feature.ExplodeMap;
import datastax.cdm.feature.FeatureFactory;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

public class TargetInsertStatement extends AbstractTargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private List<String> bindColumnNames;
    private List<Integer> bindColumnIndexes;

    public TargetInsertStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper, CqlSession session) {
        super(propertyHelper, cqlHelper, session);
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
        if (null == originRow)
            throw new RuntimeException("Origin row is null");
        if (usingCounter)
            throw new RuntimeException("Cannot INSERT onto a counter table, use UPDATE instead");

        checkBindInputs(ttl, writeTime, explodeMapKey, explodeMapValue);
        BoundStatement boundStatement = prepareStatement().bind();

        int currentBindIndex = 0;
        Object bindValue = null;

        for (int targetIndex = 0; targetIndex < targetColumnTypes.size(); targetIndex++) {
            if (!bindColumnIndexes.contains(targetIndex)) {
                continue;
            }

            MigrateDataType targetDataType = targetColumnTypes.get(targetIndex);
            Integer originIndex = ColumnsKeysTypes.getTargetToOriginColumnIndexes(propertyHelper).get(targetIndex);
            MigrateDataType originDataType = null;
            try {
                if (targetIndex==explodeMapKeyIndex) {
                    bindValue = explodeMapKey;
                    originDataType = explodeMapKeyDataType;
                }
                else if (targetIndex==explodeMapValueIndex) {
                    bindValue = explodeMapValue;
                    originDataType = explodeMapValueDataType;
                }
                else if (originIndex < 0) {
                    continue;
                }
                else {
                    originDataType = originColumnTypes.get(originIndex);
                    Object originValue = cqlHelper.getData(originDataType, originIndex, originRow);
                    if (targetDataType.hasUDT() && udtMappingEnabled) bindValue = udtMapper.convert(true, originIndex, originValue);
                    else bindValue = originValue;
                }

                bindValue = (targetDataType.equals(originDataType)) ? bindValue : MigrateDataType.convert(bindValue, originDataType, targetDataType, cqlHelper.getCodecRegistry());
                boundStatement = boundStatement.set(currentBindIndex++, bindValue, targetDataType.getTypeClass());
            }
            catch (Exception e) {
                logger.error("Error trying to bind value:" + bindValue + " to column:" + targetColumnNames.get(targetIndex) + " of targetDataType:" + targetDataType+ "/" + targetDataType.getTypeClass().getName() +
                        " at column index:" + targetIndex + " with current bind index " + (currentBindIndex-1) + " from originIndex:" + originIndex + " " + originDataType + "/" + originDataType.getTypeClass().getName());
                throw e;
            }
        }

        if (usingTTL) {
            boundStatement = boundStatement.set(currentBindIndex++, ttl, Integer.class);
        }
        if (usingWriteTime) {
            boundStatement = boundStatement.set(currentBindIndex++, writeTime, Long.class);
        }

        return boundStatement
                .setConsistencyLevel(cqlHelper.getWriteConsistencyLevel())
                .setTimeout(Duration.ofSeconds(10));
    }

    protected String buildStatement() {
        String targetUpdateCQL;
        String valuesList = "";

        setBindColumnNamesAndIndexes();

        for (String key : bindColumnNames) {
            if (valuesList.isEmpty()) {
                valuesList = "?";
            } else {
                valuesList += ",?";
            }

            if (FeatureFactory.isEnabled(explodeMapFeature)) {
                if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME))) explodeMapKeyIndex = bindIndex;
                else if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.VALUE_COLUMN_NAME))) explodeMapValueIndex = bindIndex;
            }

            bindIndex++;
        }

        if (FeatureFactory.isEnabled(constantColumnFeature)) {
            // constants are not bound, so no need to increment the bind index
            valuesList += "," + constantColumnFeature.getAsString(ConstantColumns.Property.COLUMN_VALUES);
        }

        targetUpdateCQL = "INSERT INTO " + ColumnsKeysTypes.getTargetKeyspaceTable(propertyHelper) +
                " (" + PropertyHelper.asString(bindColumnNames, KnownProperties.PropertyType.STRING_LIST) +
                (FeatureFactory.isEnabled(constantColumnFeature) ? "," + constantColumnFeature.getAsString(ConstantColumns.Property.COLUMN_NAMES) : "") +
                ") VALUES (" + valuesList + ")";

        targetUpdateCQL += usingTTLTimestamp();

        return targetUpdateCQL;
    }

    private void setBindColumnNamesAndIndexes() {
        this.bindColumnNames = new ArrayList<>();
        this.bindColumnIndexes = new ArrayList<>();

        for (String targetColumnName : this.targetColumnNames) {
            if (!FeatureFactory.isEnabled(this.constantColumnFeature)
                    || !this.constantColumnFeature.getStringList(ConstantColumns.Property.COLUMN_NAMES).contains(targetColumnName)) {
                this.bindColumnNames.add(targetColumnName);
                this.bindColumnIndexes.add(this.targetColumnNames.indexOf(targetColumnName));
            }
        }
    }
}
