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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

public class TargetInsertStatement extends TargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final boolean logDebug = logger.isDebugEnabled();

    private List<String> bindColumnNames;
    private List<Integer> bindColumnIndexes;

    public TargetInsertStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey,
            Object explodeMapValue) {
        if (null == originRow)
            throw new RuntimeException("Origin row is null");
        if (usingCounter)
            throw new RuntimeException("Cannot INSERT onto a counter table, use UPDATE instead");

        checkBindInputs(ttl, writeTime, explodeMapKey, explodeMapValue);
        BoundStatement boundStatement = prepareStatement().bind();

        int currentBindIndex = 0;
        Object bindValue = null;

        if (logDebug)
            logger.debug("bind using conversions: {}", cqlTable.getOtherCqlTable().getConversions());
        for (int targetIndex = 0; targetIndex < targetColumnTypes.size(); targetIndex++) {
            if (!bindColumnIndexes.contains(targetIndex)) {
                // this happens with constant columns, for example
                continue;
            }
            try {
                if (targetIndex == explodeMapKeyIndex) {
                    bindValue = explodeMapKey;
                } else if (targetIndex == explodeMapValueIndex) {
                    bindValue = explodeMapValue;
                } else if (targetIndex == extractJsonFeature.getTargetColumnIndex()) {
                    int originIndex = extractJsonFeature.getOriginColumnIndex();
                    bindValue = extractJsonFeature.extract(originRow.getString(originIndex));
                } else {
                    int originIndex = cqlTable.getCorrespondingIndex(targetIndex);
                    if (originIndex < 0) // we don't have data to bind for this column; continue to the next targetIndex
                    {
                        currentBindIndex++;
                        continue;
                    }
                    bindValue = cqlTable.getOtherCqlTable().getAndConvertData(originIndex, originRow);
                }

                if (!(null == bindValue)) {
                    boundStatement = boundStatement.set(currentBindIndex, bindValue,
                            cqlTable.getBindClass(targetIndex));
                }
                currentBindIndex++;
            } catch (Exception e) {
                logger.error(
                        "Error trying to bind value: {} of class: {} to column: {} of targetDataType: {}/{} at column index: {} and bind index: {} of statement: {}",
                        bindValue, (null == bindValue ? "unknown" : bindValue.getClass().getName()),
                        targetColumnNames.get(targetIndex), targetColumnTypes.get(targetIndex),
                        cqlTable.getBindClass(targetIndex).getName(), targetIndex, (currentBindIndex - 1),
                        this.getCQL());
                throw new RuntimeException("Error trying to bind value: ", e);
            }
        }

        if (usingTTL) {
            boundStatement = boundStatement.set(currentBindIndex++, ttl, Integer.class);
        }
        if (usingWriteTime) {
            boundStatement = boundStatement.set(currentBindIndex++, writeTime, Long.class);
        }

        return boundStatement.setConsistencyLevel(cqlTable.getWriteConsistencyLevel())
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

            bindIndex++;
        }

        if (null != constantColumnValues && !constantColumnValues.isEmpty()) {
            // constants are not bound, so no need to increment the bind index
            valuesList += "," + PropertyHelper.asString(constantColumnValues, KnownProperties.PropertyType.STRING_LIST);
        }

        targetUpdateCQL = "INSERT INTO " + cqlTable.getKeyspaceTable() + " ("
                + PropertyHelper.asString(bindColumnNames, KnownProperties.PropertyType.STRING_LIST)
                + (null != constantColumnNames && !constantColumnNames.isEmpty()
                        ? "," + PropertyHelper.asString(constantColumnNames, KnownProperties.PropertyType.STRING_LIST)
                        : "")
                + ") VALUES (" + valuesList + ")";

        targetUpdateCQL += usingTTLTimestamp();

        return targetUpdateCQL;
    }

    private void setBindColumnNamesAndIndexes() {
        this.bindColumnNames = new ArrayList<>();
        this.bindColumnIndexes = new ArrayList<>();

        for (String targetColumnName : this.targetColumnNames) {
            if (null == constantColumnNames || !constantColumnNames.contains(targetColumnName)) {
                this.bindColumnNames.add(targetColumnName);
                this.bindColumnIndexes.add(this.targetColumnNames.indexOf(targetColumnName));
            }
        }
    }
}
