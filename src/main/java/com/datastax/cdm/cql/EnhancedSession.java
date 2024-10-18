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
package com.datastax.cdm.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.codec.CodecFactory;
import com.datastax.cdm.cql.codec.Codecset;
import com.datastax.cdm.cql.statement.OriginSelectByPKStatement;
import com.datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import com.datastax.cdm.cql.statement.TargetInsertStatement;
import com.datastax.cdm.cql.statement.TargetSelectByPKStatement;
import com.datastax.cdm.cql.statement.TargetUpdateStatement;
import com.datastax.cdm.cql.statement.TargetUpsertStatement;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;

public class EnhancedSession {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();

    private final PropertyHelper propertyHelper;
    private final CqlSession cqlSession;
    private final CqlTable cqlTable;
    private final boolean isOrigin;
    private PKFactory pkFactory;

    public EnhancedSession(PropertyHelper propertyHelper, CqlSession cqlSession, boolean isOrigin) {
        this.propertyHelper = propertyHelper;
        this.cqlSession = initSession(propertyHelper, cqlSession);
        this.isOrigin = isOrigin;

        cqlTable = new CqlTable(propertyHelper, isOrigin, cqlSession);
    }

    public void setPKFactory(PKFactory pkFactory) {
        this.pkFactory = pkFactory;
    }

    public PKFactory getPKFactory() {
        return this.pkFactory;
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public CqlTable getCqlTable() {
        return cqlTable;
    }

    public OriginSelectByPartitionRangeStatement getOriginSelectByPartitionRangeStatement() {
        if (!isOrigin)
            throw new RuntimeException("This is not an origin session");
        return new OriginSelectByPartitionRangeStatement(propertyHelper, this);
    }

    public OriginSelectByPKStatement getOriginSelectByPKStatement() {
        if (!isOrigin)
            throw new RuntimeException("This is not an origin session");
        return new OriginSelectByPKStatement(propertyHelper, this);
    }

    public TargetSelectByPKStatement getTargetSelectByPKStatement() {
        if (isOrigin)
            throw new RuntimeException("This is not a target session");
        return new TargetSelectByPKStatement(propertyHelper, this);
    }

    public TargetUpsertStatement getTargetUpsertStatement() {
        if (isOrigin)
            throw new RuntimeException("This is not a target session");
        if (cqlTable.isCounterTable())
            return new TargetUpdateStatement(propertyHelper, this);
        else
            return new TargetInsertStatement(propertyHelper, this);
    }

    private CqlSession initSession(PropertyHelper propertyHelper, CqlSession session) {
        // BIGINT_BIGINTEGER codec is always needed to compare C* writetimes in collection columns
        List<String> codecList = new ArrayList<>(Arrays.asList("BIGINT_BIGINTEGER"));

        if (null != propertyHelper.getStringList(KnownProperties.TRANSFORM_CODECS))
            codecList.addAll(propertyHelper.getStringList(KnownProperties.TRANSFORM_CODECS));
        MutableCodecRegistry registry = (MutableCodecRegistry) session.getContext().getCodecRegistry();

        codecList.stream().map(Codecset::valueOf).map(codec -> CodecFactory.getCodecPair(propertyHelper, codec))
                .flatMap(List::stream).forEach(registry::register);

        return session;
    }

}
