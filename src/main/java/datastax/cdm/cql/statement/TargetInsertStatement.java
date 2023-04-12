package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.feature.ConstantColumns;
import datastax.cdm.feature.ExplodeMap;
import datastax.cdm.feature.FeatureFactory;
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

    public TargetInsertStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
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
        Object bindValue;
        for (int index = 0; index < targetColumnTypes.size(); index++) {
            if (!bindColumnIndexes.contains(index)) {
                continue;
            }

            MigrateDataType dataType = targetColumnTypes.get(index);

            if (index==explodeMapKeyIndex) bindValue = explodeMapKey;
            else if (index==explodeMapValueIndex) bindValue = explodeMapValue;
            else if (index < originColumnTypes.size()) bindValue = cqlHelper.getData(dataType, index, originRow);
            else continue;

            boundStatement = boundStatement.set(currentBindIndex++, bindValue, dataType.getTypeClass());
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

        targetUpdateCQL = "INSERT INTO " + propertyHelper.getAsString(KnownProperties.TARGET_KEYSPACE_TABLE) +
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
