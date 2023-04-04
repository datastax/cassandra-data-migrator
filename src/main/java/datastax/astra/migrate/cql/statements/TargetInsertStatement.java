package datastax.astra.migrate.cql.statements;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.cql.features.ConstantColumns;
import datastax.astra.migrate.cql.features.ExplodeMap;
import datastax.astra.migrate.cql.features.FeatureFactory;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;

import java.time.Duration;

public class TargetInsertStatement extends AbstractTargetUpsertStatement {

    public TargetInsertStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Long ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
        if (null == originRow)
            throw new RuntimeException("Origin row is null");
        if (usingCounter)
            throw new RuntimeException("Cannot INSERT onto a counter table, use UPDATE instead");

        checkBindInputs(writeTime, ttl, explodeMapKey, explodeMapValue);
        BoundStatement boundStatement = prepareStatement().bind();

        for (int index = 0; index < bindIndex; index++) {
            MigrateDataType dataType = targetColumnTypes.get(index);
            Object bindValue;

            if (index==ttlBindIndex) bindValue = ttl;
            else if (index==writeTimeBindIndex) bindValue = writeTime;
            else if (index==explodeMapKeyIndex) bindValue = explodeMapKey;
            else if (index==explodeMapValueIndex) bindValue = explodeMapValue;
            else bindValue = cqlHelper.getData(dataType, index, originRow);

            boundStatement = boundStatement.set(index, bindValue, dataType.getTypeClass());
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

        if (usingTTL || usingWriteTime)
            targetUpdateCQL += " USING ";
        if (usingTTL) {
            targetUpdateCQL += "TTL ?";
            ttlBindIndex = bindIndex++;
        }
        if (usingTTL && usingWriteTime)
            targetUpdateCQL += " AND ";
        if (usingWriteTime) {
            targetUpdateCQL += "TIMESTAMP ?";
            writeTimeBindIndex = bindIndex++;
        }
        return targetUpdateCQL;
    }
}
