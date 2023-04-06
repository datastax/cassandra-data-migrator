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

import java.time.Duration;

public class TargetInsertStatement extends AbstractTargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

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
            MigrateDataType dataType = targetColumnTypes.get(index);

            if (index==explodeMapKeyIndex) bindValue = explodeMapKey;
            else if (index==explodeMapValueIndex) bindValue = explodeMapValue;
            else bindValue = cqlHelper.getData(dataType, index, originRow);

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
        for (String key : targetColumnNames) {
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
                " (" + propertyHelper.getAsString(KnownProperties.TARGET_COLUMN_NAMES) +
                (FeatureFactory.isEnabled(constantColumnFeature) ? "," + constantColumnFeature.getAsString(ConstantColumns.Property.COLUMN_NAMES) : "") +
                ") VALUES (" + valuesList + ")";

        targetUpdateCQL += usingTTLTimestamp();

        return targetUpdateCQL;
    }
}
