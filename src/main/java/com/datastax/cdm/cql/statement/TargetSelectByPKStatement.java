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

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.EnhancedPK;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

public class TargetSelectByPKStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public TargetSelectByPKStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);
        this.statement = buildStatement();
    }

    public Record getRecord(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null==boundStatement)
            return null;

        ResultSet resultSet = session.getCqlSession().execute(boundStatement);
        if (null==resultSet)
            return null;

        Row row = resultSet.one();
        if (null==row)
            return null;

        return new Record(pk, null, row);
    }

    public CompletionStage<AsyncResultSet> getAsyncResult(EnhancedPK pk) {
        BoundStatement boundStatement = bind(pk);
        if (null==boundStatement)
            return null;
        return session.getCqlSession().executeAsync(boundStatement);
    }

    private BoundStatement bind(EnhancedPK pk) {
        BoundStatement boundStatement = prepareStatement().bind()
                .setConsistencyLevel(cqlTable.getReadConsistencyLevel());

        boundStatement = session.getPKFactory().bindWhereClause(PKFactory.Side.TARGET, pk, boundStatement, 0);
        return boundStatement;
    }

    private String buildStatement() {
        return "SELECT " + PropertyHelper.asString(cqlTable.getColumnNames(true), KnownProperties.PropertyType.STRING_LIST)
                + " FROM " + cqlTable.getKeyspaceTable()
                + " WHERE " + session.getPKFactory().getWhereClause(PKFactory.Side.TARGET);
    }
}