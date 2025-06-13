/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.cdm.cql.statement;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.ConstantColumns;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.feature.ExtractJson;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.WritetimeTTL;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;

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

    protected ExtractJson extractJsonFeature;

    protected abstract String buildStatement();

    protected abstract BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime,
            Object explodeMapKey, Object explodeMapValue);

    public TargetUpsertStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);

        constantColumnFeature = (ConstantColumns) cqlTable.getFeature(Featureset.CONSTANT_COLUMNS);
        explodeMapFeature = (ExplodeMap) cqlTable.getFeature(Featureset.EXPLODE_MAP);
        extractJsonFeature = (ExtractJson) cqlTable.getFeature(Featureset.EXTRACT_JSON);

        setTTLAndWriteTimeBooleans();
        targetColumnNames.addAll(cqlTable.getColumnNames(true));
        targetColumnTypes.addAll(cqlTable.getColumnCqlTypes());
        originColumnNames.addAll(cqlTable.getOtherCqlTable().getColumnNames(true));
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

    public BoundStatement bindRecord(Record record) {
        if (null == record)
            throw new RuntimeException("record is null");

        EnhancedPK pk = record.getPk();
        Row originRow = record.getOriginRow();
        Row targetRow = record.getTargetRow();

        return bind(originRow, targetRow, pk.getTTL(), pk.getWriteTimestamp(), pk.getExplodeMapKey(),
                pk.getExplodeMapValue());
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
            throw new RuntimeException(
                    KnownProperties.ORIGIN_WRITETIME_NAMES + " specified, but no WriteTime value was provided");

        if (null != explodeMapFeature && explodeMapFeature.isEnabled()) {
            if (null == explodeMapKey)
                throw new RuntimeException("ExplodeMap is enabled, but no map key was provided");
            else if (!cqlTable.getBindClass(explodeMapKeyIndex).isAssignableFrom(explodeMapKey.getClass()))
                throw new RuntimeException(
                        "ExplodeMap is enabled, but the map key type provided " + explodeMapKey.getClass().getName()
                                + " is not compatible with " + cqlTable.getBindClass(explodeMapKeyIndex).getName());

            if (null == explodeMapValue)
                throw new RuntimeException("ExplodeMap is enabled, but no map value was provided");
            else if (!cqlTable.getBindClass(explodeMapValueIndex).isAssignableFrom(explodeMapValue.getClass()))
                throw new RuntimeException(
                        "ExplodeMap is enabled, but the map value type provided " + explodeMapValue.getClass().getName()
                                + " is not compatible with " + cqlTable.getBindClass(explodeMapValueIndex).getName());
        }

        // this is the only place this variable is modified, so suppress the warning
        // noinspection SynchronizeOnNonFinalField
        synchronized (this.haveCheckedBindInputsOnce) {
            this.haveCheckedBindInputsOnce = true;
        }
    }

}
