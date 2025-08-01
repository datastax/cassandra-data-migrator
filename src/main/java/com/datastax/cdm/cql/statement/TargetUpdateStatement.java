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
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

public class TargetUpdateStatement extends TargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<Integer> columnIndexesToBind;

    public TargetUpdateStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
        this.columnIndexesToBind = calcColumnIndexesToBind();
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey,
            Object explodeMapValue) {
        // We reference the originRow and convert it to the target type.
        // We need the targetRow
        if (null == originRow)
            throw new RuntimeException("originRow is null");

        checkBindInputs(ttl, writeTime, explodeMapKey, explodeMapValue);

        BoundStatement boundStatement = prepareStatement().bind();

        int currentBindIndex = 0;
        if (usingTTL) {
            boundStatement = boundStatement.set(currentBindIndex++, ttl, Integer.class);
        }
        if (usingWriteTime) {
            boundStatement = boundStatement.set(currentBindIndex++, writeTime, Long.class);
        }

        Object originValue, targetValue;
        Object bindValueTarget = null;
        for (int targetIndex : columnIndexesToBind) {
            int originIndex = cqlTable.getCorrespondingIndex(targetIndex);

            try {
                if (usingCounter && counterIndexes.contains(targetIndex)) {
                    originValue = cqlTable.getOtherCqlTable().getData(originIndex, originRow);
                    if (null == originValue) {
                        currentBindIndex++;
                        continue;
                    }
                    targetValue = (null == targetRow ? 0L : cqlTable.getData(targetIndex, targetRow));
                    bindValueTarget = ((Long) originValue - (null == targetValue ? 0L : (Long) targetValue));
                } else if (targetIndex == explodeMapKeyIndex) {
                    bindValueTarget = explodeMapKey;
                } else if (targetIndex == explodeMapValueIndex) {
                    bindValueTarget = explodeMapValue;
                } else if (targetIndex == extractJsonFeature.getTargetColumnIndex()) {
                    originIndex = extractJsonFeature.getOriginColumnIndex();
                    bindValueTarget = extractJsonFeature.extract(originRow.getString(originIndex));
                } else {
                    if (originIndex < 0) // we don't have data to bind for this column; continue to the next targetIndex
                    {
                        currentBindIndex++;
                        continue;
                    }
                    bindValueTarget = cqlTable.getOtherCqlTable().getAndConvertData(originIndex, originRow);
                }

                if (!(null == bindValueTarget)) {
                    boundStatement = boundStatement.set(currentBindIndex, bindValueTarget,
                            cqlTable.getBindClass(targetIndex));
                }
                currentBindIndex++;
            } catch (Exception e) {
                logger.error("Error trying to bind value:" + bindValueTarget + " to column:"
                        + targetColumnNames.get(targetIndex) + " of targetDataType:"
                        + targetColumnTypes.get(targetIndex) + "/" + cqlTable.getBindClass(targetIndex).getName()
                        + " at column index:" + targetIndex);
                throw new RuntimeException("Error trying to bind value: ", e);
            }
        }

        PKFactory pkFactory = session.getPKFactory();
        EnhancedPK pk = pkFactory.getTargetPK(originRow);
        boundStatement = pkFactory.bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, currentBindIndex);

        return boundStatement.setConsistencyLevel(cqlTable.getWriteConsistencyLevel())
                .setTimeout(Duration.ofSeconds(10));
    }

    @Override
    protected String buildStatement() {
        PKFactory pkFactory = session.getPKFactory();
        StringBuilder targetUpdateCQL = new StringBuilder("UPDATE ");
        targetUpdateCQL.append(cqlTable.getKeyspaceTable());
        targetUpdateCQL.append(usingTTLTimestamp());
        targetUpdateCQL.append(" SET ");
        int currentColumn = 0;
        for (String key : targetColumnNames) {
            if (!pkFactory.getPKNames(PKFactory.Side.TARGET, false).contains(key)) {
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

    private List<Integer> calcColumnIndexesToBind() {
        List<Integer> rtn = new ArrayList<>();
        List<String> pkNames = cqlTable.getPKNames(false);
        int currentColumn = 0;
        for (String key : cqlTable.getColumnNames(false)) {
            if (!pkNames.contains(key)) {
                rtn.add(currentColumn);
            }
            currentColumn++;
        }
        return rtn;
    }

}
