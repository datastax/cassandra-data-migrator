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

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class BaseCdmStatement {

    protected IPropertyHelper propertyHelper;
    protected CqlTable cqlTable;
    protected String statement = "";
    protected EnhancedSession session;

    protected List<String> resultColumns = new ArrayList<>();

    public BaseCdmStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        if (null == propertyHelper || null == session || null == session.getCqlTable())
            throw new RuntimeException("PropertyHelper or EnhancedSession or EnhancedSession.getCqlTable() is not set");
        this.propertyHelper = propertyHelper;
        this.cqlTable = session.getCqlTable();
        this.session = session;
    }

    public PreparedStatement prepareStatement() {
        if (null == session || null == session.getCqlSession())
            throw new RuntimeException("Session is not set");
        if (null == statement || statement.isEmpty())
            throw new RuntimeException("Statement is not set");
        return session.getCqlSession().prepare(statement);
    }

    public String getCQL() {
        return statement;
    }

}
