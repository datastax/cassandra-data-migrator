package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.ConstantColumns;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.WritetimeTTL;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public abstract class TargetUpsertStatement extends BaseCdmStatement {
    protected final List<String> targetColumnNames = new ArrayList<>();
    protected final List<String> originColumnNames = new ArrayList<>();
    protected final List<String> constantColumnNames = new ArrayList<>();
    protected final List<String> constantColumnValues = new ArrayList<>();

    protected final List<DataType> targetColumnTypes = new ArrayList<>();
    protected final List<DataType> originColumnTypes = new ArrayList<>();

    protected final List<Integer> counterIndexes;

    protected boolean usingCounter = false;
    protected boolean usingTTL = false;
    protected boolean usingWriteTime = false;
    protected ConstantColumns constantColumnFeature;
    protected ExplodeMap explodeMapFeature;

    protected int bindIndex = 0;
    protected int explodeMapKeyIndex = -1;
    protected int explodeMapValueIndex = -1;
    private Boolean haveCheckedBindInputsOnce = false;

    public TargetUpsertStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);

        constantColumnFeature = (ConstantColumns) cqlTable.getFeature(Featureset.CONSTANT_COLUMNS);
        explodeMapFeature = (ExplodeMap) cqlTable.getFeature(Featureset.EXPLODE_MAP);

        setTTLAndWriteTimeBooleans();
        targetColumnNames.addAll(cqlTable.getColumnNames(false));
        targetColumnTypes.addAll(cqlTable.getColumnCqlTypes());
        originColumnNames.addAll(cqlTable.getOtherCqlTable().getColumnNames(false));
        originColumnTypes.addAll(cqlTable.getOtherCqlTable().getColumnCqlTypes());
        setConstantColumns();
        if (null != explodeMapFeature && explodeMapFeature.isEnabled()) {
            this.explodeMapKeyIndex = explodeMapFeature.getKeyColumnIndex();
            this.explodeMapValueIndex = explodeMapFeature.getValueColumnIndex();
        }
        this.counterIndexes = cqlTable.getCounterIndexes();
        this.usingCounter = !counterIndexes.isEmpty();

        this.statement = buildStatement();
    }

    protected abstract String buildStatement();

    protected abstract BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue);

    public BoundStatement bindRecord(Record record) {
        if (null == record)
            throw new RuntimeException("record is null");

        EnhancedPK pk = record.getPk();
        Row originRow = record.getOriginRow();
        Row targetRow = record.getTargetRow();

        return bind(originRow, targetRow, pk.getTTL(), pk.getWriteTimestamp(), pk.getExplodeMapKey(), pk.getExplodeMapValue());
    }

    public CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
        return session.getCqlSession().executeAsync(statement);
    }

    public ResultSet putRecord(Record record) {
        BoundStatement boundStatement = bindRecord(record);
        return session.getCqlSession().execute(boundStatement);
    }

    protected String usingTTLTimestamp() {
        StringBuilder sb;
        if (usingTTL || usingWriteTime)
            sb = new StringBuilder(" USING ");
        else
            return "";

        if (usingTTL)
            sb.append("TTL ?");

        if (usingTTL && usingWriteTime)
            sb.append(" AND ");

        if (usingWriteTime)
            sb.append("TIMESTAMP ?");

        return sb.toString();
    }

    private void setConstantColumns() {
        if (null != constantColumnFeature && constantColumnFeature.isEnabled()) {
            constantColumnNames.addAll(constantColumnFeature.getNames());
            constantColumnValues.addAll(constantColumnFeature.getValues());
        }

        if (constantColumnNames.size() != constantColumnValues.size()) {
            throw new RuntimeException("Constant column names and values are not the same size.");
        }
    }

    private void setTTLAndWriteTimeBooleans() {
        usingTTL = false;
        usingWriteTime = false;
        WritetimeTTL wtFeature = (WritetimeTTL) cqlTable.getFeature(Featureset.WRITETIME_TTL);

        if (null != wtFeature && wtFeature.isEnabled()) {
            usingTTL = wtFeature.hasTTLColumns();
            usingWriteTime = wtFeature.hasWritetimeColumns();
        }
    }

    protected void checkBindInputs(Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
        if (haveCheckedBindInputsOnce)
            return;

        if (usingTTL && null == ttl)
            throw new RuntimeException(KnownProperties.ORIGIN_TTL_NAMES + " specified, but no TTL value was provided");

        if (usingWriteTime && null == writeTime)
            throw new RuntimeException(KnownProperties.ORIGIN_WRITETIME_NAMES + " specified, but no WriteTime value was provided");

        if (null != explodeMapFeature && explodeMapFeature.isEnabled()) {
            if (null == explodeMapKey)
                throw new RuntimeException("ExplodeMap is enabled, but no map key was provided");
            else if (!cqlTable.getBindClass(explodeMapKeyIndex).isAssignableFrom(explodeMapKey.getClass()))
                throw new RuntimeException("ExplodeMap is enabled, but the map key type provided " + explodeMapKey.getClass().getName() + " is not compatible with " + cqlTable.getBindClass(explodeMapKeyIndex).getName());

            if (null == explodeMapValue)
                throw new RuntimeException("ExplodeMap is enabled, but no map value was provided");
            else if (!cqlTable.getBindClass(explodeMapValueIndex).isAssignableFrom(explodeMapValue.getClass()))
                throw new RuntimeException("ExplodeMap is enabled, but the map value type provided " + explodeMapValue.getClass().getName() + " is not compatible with " + cqlTable.getBindClass(explodeMapValueIndex).getName());
        }

        // this is the only place this variable is modified, so suppress the warning
        //noinspection SynchronizeOnNonFinalField
        synchronized (this.haveCheckedBindInputsOnce) {
            this.haveCheckedBindInputsOnce = true;
        }
    }

}
