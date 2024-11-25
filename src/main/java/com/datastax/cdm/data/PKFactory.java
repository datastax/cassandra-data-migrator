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
package com.datastax.cdm.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.ConstantColumns;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.feature.FeatureFactory;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.WritetimeTTL;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

public class PKFactory {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();

    private enum LookupMethod {
        ORIGIN_COLUMN, CONSTANT_COLUMN, EXPLODE_MAP
    }

    public enum Side {
        ORIGIN, TARGET
    }

    private final CqlTable originTable;
    private final CqlTable targetTable;
    private final WritetimeTTL writetimeTTLFeature;

    private final List<Integer> targetPKIndexesToBind;
    private final List<LookupMethod> targetPKLookupMethods;
    private final List<Object> targetDefaultValues;
    private final String targetWhereClause;

    private final List<Integer> originPKIndexesToBind;
    private final List<LookupMethod> originPKLookupMethods;
    private final String originWhereClause;

    private final Integer explodeMapOriginColumnIndex;
    private final Integer explodeMapTargetKeyColumnIndex;
    private final Integer explodeMapTargetPKIndex;
    private final ExplodeMap explodeMapFeature;

    public PKFactory(PropertyHelper propertyHelper, CqlTable originTable, CqlTable targetTable) {

        this.originTable = originTable;
        this.targetTable = targetTable;
        this.writetimeTTLFeature = (WritetimeTTL) originTable.getFeature(Featureset.WRITETIME_TTL);

        this.targetPKLookupMethods = new ArrayList<>();
        this.targetDefaultValues = new ArrayList<>();
        for (int i = 0; i < targetTable.getPKNames(false).size(); i++) {
            targetPKLookupMethods.add(null);
            targetDefaultValues.add(null);
        }

        this.originPKLookupMethods = new ArrayList<>();
        for (int i = 0; i < originTable.getPKNames(false).size(); i++) {
            originPKLookupMethods.add(null);
        }

        setOriginColumnLookupMethod(propertyHelper);
        setConstantColumns();

        this.explodeMapTargetKeyColumnIndex = setExplodeMapMethods_getTargetKeyColumnIndex();
        this.explodeMapOriginColumnIndex = getExplodeMapOriginColumnIndex();
        this.explodeMapTargetPKIndex = targetPKLookupMethods.indexOf(LookupMethod.EXPLODE_MAP);
        this.explodeMapFeature = (ExplodeMap) targetTable.getFeature(Featureset.EXPLODE_MAP);

        // These need to be set once all the features have been processed
        this.targetPKIndexesToBind = getIndexesToBind(Side.TARGET);
        this.originPKIndexesToBind = getIndexesToBind(Side.ORIGIN);
        this.targetWhereClause = getWhereClause(Side.TARGET);
        this.originWhereClause = getWhereClause(Side.ORIGIN);
    }

    public EnhancedPK getTargetPK(Row originRow) {
        List<Object> newValues = getTargetPKValuesFromOriginColumnLookupMethod(originRow, targetDefaultValues);
        if (logDebug)
            logger.debug("getTargetPK: newValues: {}; {}; explodeMapTargetKeyColumnIndex={}", newValues,
                    null == writetimeTTLFeature ? WritetimeTTL.class.getSimpleName() + ":null" : writetimeTTLFeature,
                    explodeMapTargetKeyColumnIndex);
        Long originWriteTimeStamp = null;
        Integer originTTL = null;
        if (FeatureFactory.isEnabled(writetimeTTLFeature)) {
            if (writetimeTTLFeature.getCustomWritetime() > 0) {
                originWriteTimeStamp = writetimeTTLFeature.getCustomWritetime();
            } else {
                originWriteTimeStamp = writetimeTTLFeature.getLargestWriteTimeStamp(originRow);
            }
            if (writetimeTTLFeature.getCustomTTL() > 0) {
                originTTL = writetimeTTLFeature.getCustomTTL().intValue();
            } else {
                originTTL = writetimeTTLFeature.getLargestTTL(originRow);
            }
        }
        if (explodeMapTargetKeyColumnIndex < 0) {
            return new EnhancedPK(this, newValues, getPKClasses(Side.TARGET), originTTL, originWriteTimeStamp);
        } else {
            Map<Object, Object> explodeMap = getExplodeMap(originRow);
            return new EnhancedPK(this, newValues, getPKClasses(Side.TARGET), originTTL, originWriteTimeStamp,
                    explodeMap);
        }
    }

    public String getWhereClause(Side side) {
        StringBuilder sb;
        List<String> pkNames;
        switch (side) {
        case ORIGIN:
            if (null != originWhereClause && !originWhereClause.isEmpty())
                return originWhereClause;
            sb = new StringBuilder();
            pkNames = originTable.getPKNames(true);
            for (int i = 0; i < pkNames.size(); i++) {
                LookupMethod method = originPKLookupMethods.get(i);
                String name = pkNames.get(i);

                // On origin PK, we don't bind anything other than ORIGIN_COLUMN
                if (method == LookupMethod.ORIGIN_COLUMN) {
                    if (sb.length() > 0)
                        sb.append(" AND ");
                    sb.append(name).append("=?");
                }
            }
            return sb.toString();
        case TARGET:
            if (null != targetWhereClause && !targetWhereClause.isEmpty())
                return targetWhereClause;
            sb = new StringBuilder();
            pkNames = targetTable.getPKNames(true);
            for (int i = 0; i < pkNames.size(); i++) {
                LookupMethod method = targetPKLookupMethods.get(i);
                String name = pkNames.get(i);
                Object defaultValue = targetDefaultValues.get(i);

                if (null == method)
                    continue;
                switch (method) {
                case ORIGIN_COLUMN:
                case EXPLODE_MAP:
                    if (sb.length() > 0)
                        sb.append(" AND ");
                    sb.append(name).append("=?");
                    break;
                case CONSTANT_COLUMN:
                    if (null != defaultValue) {
                        if (sb.length() > 0)
                            sb.append(" AND ");
                        sb.append(name).append("=").append(defaultValue);
                    }
                    break;
                }
            }
            return sb.toString();
        }
        return null;
    }

    public BoundStatement bindWhereClause(Side side, EnhancedPK pk, BoundStatement boundStatement,
            int startingBindIndex) {
        List<Integer> indexesToBind;
        CqlTable table;
        switch (side) {
        case ORIGIN:
            indexesToBind = originPKIndexesToBind;
            table = originTable;
            break;
        case TARGET:
            indexesToBind = targetPKIndexesToBind;
            table = targetTable;
            break;
        default:
            throw new RuntimeException("Unknown side: " + side);
        }

        if (pk.isError() || pk.getPKValues().size() != table.getPKClasses().size())
            throw new RuntimeException(
                    "PK is in Error state, or the number of values does not match the number of bind types");

        for (int i = 0; i < indexesToBind.size(); i++) {
            int index = indexesToBind.get(i);
            boundStatement = boundStatement.set(startingBindIndex++, pk.getPKValues().get(index),
                    table.getPKClasses().get(index));
        }

        return boundStatement;
    }

    public List<String> getPKNames(Side side, boolean pretty) {
        switch (side) {
        case ORIGIN:
            return originTable.getPKNames(pretty);
        case TARGET:
            return targetTable.getPKNames(pretty);
        default:
            throw new RuntimeException("Unknown side: " + side);
        }
    }

    public List<Class> getPKClasses(Side side) {
        switch (side) {
        case ORIGIN:
            return originTable.getPKClasses();
        case TARGET:
            return targetTable.getPKClasses();
        default:
            throw new RuntimeException("Unknown side: " + side);
        }
    }

    public List<Record> toValidRecordList(Record record) {
        if (null == record || !record.isValid())
            return new ArrayList<>(0);

        List<Record> recordSet;
        if (record.getPk().canExplode()) {
            recordSet = record.getPk().explode(explodeMapFeature).stream().filter(pk -> !pk.isError())
                    .map(pk -> new Record(pk, record.getOriginRow(), record.getTargetRow()))
                    .collect(Collectors.toList());
        } else {
            recordSet = Arrays.asList(record);
        }
        return recordSet;
    }

    public Integer getExplodeMapTargetPKIndex() {
        return explodeMapTargetPKIndex;
    }

    private List<Object> getTargetPKValuesFromOriginColumnLookupMethod(Row originRow, List<Object> defaultValues) {
        List<Object> newValues = new ArrayList<>(defaultValues);
        for (int i = 0; i < targetPKLookupMethods.size(); i++) {
            if (targetPKLookupMethods.get(i) != LookupMethod.ORIGIN_COLUMN)
                continue;

            int originIndex = targetTable
                    .getCorrespondingIndex(targetTable.indexOf(targetTable.getPKNames(false).get(i)));
            Object value = originTable.getAndConvertData(originIndex, originRow);
            newValues.set(i, value);
        }
        return newValues;
    }

    private Map<Object, Object> getExplodeMap(Row originRow) {
        if (explodeMapTargetKeyColumnIndex < 0) {
            return null;
        }
        return (Map<Object, Object>) originTable.getData(explodeMapOriginColumnIndex, originRow);
    }

    // This fills the PKLookupMethods lists with either ORIGIN_COLUMN or null.
    private void setOriginColumnLookupMethod(PropertyHelper propertyHelper) {
        // Origin PK columns are expected to be found on originColumnNames; if not, it could be because
        // the origin PK defaulted from the target PK, and the column is added as part of a feature
        // (e.g. explode map). In that case, we will set the lookup to null.
        for (int i = 0; i < originTable.getPKNames(false).size(); i++) {
            if (originTable.indexOf(originTable.getPKNames(false).get(i)) >= 0)
                this.originPKLookupMethods.set(i, LookupMethod.ORIGIN_COLUMN);
        }

        // Target PK columns may or may not be found on the originColumnNames.
        for (int i = 0; i < targetTable.getPKNames(false).size(); i++) {
            if (targetTable.getCorrespondingIndex(targetTable.indexOf(targetTable.getPKNames(false).get(i))) >= 0)
                this.targetPKLookupMethods.set(i, LookupMethod.ORIGIN_COLUMN);
        }
    }

    private void setConstantColumns() {
        ConstantColumns feature = (ConstantColumns) targetTable.getFeature(Featureset.CONSTANT_COLUMNS);
        if (null != feature && feature.isEnabled()) {
            List<String> constantColumnNames = feature.getNames();
            List<String> constantColumnValues = feature.getValues();
            List<Class> constantColumnBindClasses = feature.getBindClasses();
            for (int i = 0; i < constantColumnNames.size(); i++) {
                String columnName = constantColumnNames.get(i);
                int targetPKIndex = targetTable.getPKNames(false).indexOf(columnName);
                if (targetPKIndex >= 0) {
                    this.targetDefaultValues.set(targetPKIndex, constantColumnValues.get(i));
                    this.targetPKLookupMethods.set(targetPKIndex, LookupMethod.CONSTANT_COLUMN);
                    this.targetTable.getPKClasses().set(targetPKIndex, constantColumnBindClasses.get(i));
                }
            }
        }
    }

    private Integer setExplodeMapMethods_getTargetKeyColumnIndex() {
        ExplodeMap feature = (ExplodeMap) targetTable.getFeature(Featureset.EXPLODE_MAP);
        if (null != feature && feature.isEnabled()) {
            String explodeMapKeyColumn = feature.getKeyColumnName();
            int targetPKIndex = targetTable.getPKNames(false).indexOf(explodeMapKeyColumn);
            if (targetPKIndex >= 0) {
                this.targetPKLookupMethods.set(targetPKIndex, LookupMethod.EXPLODE_MAP);
            }
            return targetPKIndex;
        }
        return -1;
    }

    private Integer getExplodeMapOriginColumnIndex() {
        ExplodeMap feature = (ExplodeMap) originTable.getFeature(Featureset.EXPLODE_MAP);
        if (null != feature && feature.isEnabled()) {
            return feature.getOriginColumnIndex();
        }
        return -1;
    }

    private List<Integer> getIndexesToBind(Side side) {
        List<Integer> indexesToBind = new ArrayList<>();
        List<LookupMethod> lookupMethods = (side == Side.ORIGIN) ? originPKLookupMethods : targetPKLookupMethods;
        for (int i = 0; i < lookupMethods.size(); i++) {
            LookupMethod method = lookupMethods.get(i);
            if (null != method && method != LookupMethod.CONSTANT_COLUMN)
                indexesToBind.add(i);
        }
        return indexesToBind;
    }
}
