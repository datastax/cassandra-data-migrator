package com.datastax.cdm.cql.statement;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TargetUpdateStatement extends TargetUpsertStatement {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final List<Integer> columnIndexesToBind;

    public TargetUpdateStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
        this.columnIndexesToBind = calcColumnIndexesToBind();
    }

    @Override
    protected BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
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

        Object bindValue = null;
        for (int targetIndex : columnIndexesToBind) {
            int originIndex = cqlTable.getCorrespondingIndex(targetIndex);

            try {
                if(usingCounter && counterIndexes.contains(targetIndex)) {
                    bindValue = cqlTable.getOtherCqlTable().getData(originIndex, originRow);
                    if (null == bindValue) {
                        currentBindIndex++;
                        continue;
                    }
                    bindValue = ((Long) bindValue - (null==targetRow ? 0 : (Long) cqlTable.getData(targetIndex, targetRow)));
                }
                else if (targetIndex== explodeMapKeyIndex) {
                    bindValue = explodeMapKey;
                }
                else if (targetIndex== explodeMapValueIndex) {
                    bindValue = explodeMapValue;
                } else {
                    if (originIndex < 0)
                        // we don't have data to bind for this column; continue to the next targetIndex
                        continue;
                    bindValue = cqlTable.getOtherCqlTable().getAndConvertData(originIndex, originRow);
                }

                boundStatement = boundStatement.set(currentBindIndex++, bindValue, cqlTable.getBindClass(targetIndex));
            }
            catch (Exception e) {
                logger.error("Error trying to bind value:" + bindValue + " to column:" + targetColumnNames.get(targetIndex) + " of targetDataType:" + targetColumnTypes.get(targetIndex)+ "/" + cqlTable.getBindClass(targetIndex).getName() + " at column index:" + targetIndex);
                throw e;
            }
        }

        PKFactory pkFactory = session.getPKFactory();
        EnhancedPK pk = pkFactory.getTargetPK(originRow);
        boundStatement = pkFactory.bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, currentBindIndex);

        return boundStatement
                .setConsistencyLevel(cqlTable.getWriteConsistencyLevel())
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
