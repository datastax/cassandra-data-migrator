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
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.WritetimeTTL;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public abstract class OriginSelectStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final Boolean writeTimestampFilterEnabled;
    private final Long minWriteTimeStampFilter;
    private final Long maxWriteTimeStampFilter;

    private final Boolean filterColumnEnabled;
    private final Integer filterColumnIndex;
    private final String filterColumnString;

    public OriginSelectStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
        super(propertyHelper, session);

        resultColumns.addAll(cqlTable.getColumnNames(false));
        if (null==resultColumns || resultColumns.isEmpty()) {
            throw new RuntimeException("No columns found in table " + cqlTable.getTableName());
        }

        WritetimeTTL writetimeTTLFeature = (WritetimeTTL) cqlTable.getFeature(Featureset.WRITETIME_TTL);
        if (null!= writetimeTTLFeature && writetimeTTLFeature.isEnabled() && writetimeTTLFeature.hasWriteTimestampFilter()) {
            writeTimestampFilterEnabled = true;
            minWriteTimeStampFilter = writetimeTTLFeature.getMinWriteTimeStampFilter();
            maxWriteTimeStampFilter = writetimeTTLFeature.getMaxWriteTimeStampFilter();
            logger.info("PARAM -- {}: {} datetime is {}", KnownProperties.FILTER_WRITETS_MIN, minWriteTimeStampFilter,
                    Instant.ofEpochMilli(minWriteTimeStampFilter / 1000));
            logger.info("PARAM -- {}: {} datetime is {}", KnownProperties.FILTER_WRITETS_MAX, maxWriteTimeStampFilter,
                    Instant.ofEpochMilli(maxWriteTimeStampFilter / 1000));
        } else {
            writeTimestampFilterEnabled = false;
            minWriteTimeStampFilter = Long.MIN_VALUE;
            maxWriteTimeStampFilter = Long.MAX_VALUE;
        }

        filterColumnString = getFilterColumnString();
        filterColumnIndex = getFilterColumnIndex();
        filterColumnEnabled = (null != filterColumnIndex && filterColumnIndex >= 0 && null != filterColumnString && !filterColumnString.isEmpty());
        if (filterColumnEnabled) {
            logger.info("PARAM -- {}: {} ", KnownProperties.FILTER_COLUMN_NAME, filterColumnString);
            logger.info("PARAM -- {}: {} ", KnownProperties.FILTER_COLUMN_VALUE, filterColumnString);
        }

        this.statement = buildStatement();
    }

    public ResultSet execute(BoundStatement boundStatement) {
        return session.getCqlSession().execute(boundStatement);
    };

    protected boolean isRecordValid(Record record) {
        if (null == record) {
            return false;
        }
        if (record.getPk().isError()) {
            logger.error("PK {} is in error state: {}", record.getPk(), record.getPk().getMessages());
            return false;
        }
        if (record.getPk().isWarning()) {
            logger.warn("PK {} is in warning state: {}", record.getPk(), record.getPk().getMessages());
        }
        return true;
    }

    public abstract BoundStatement bind(Object... binds);
    protected abstract String whereBinds();

    public boolean shouldFilterRecord(Record record) {
        if (null==record || !isRecordValid(record))
            return true;

        if (this.filterColumnEnabled) {
            String col = (String) cqlTable.getData(this.filterColumnIndex, record.getOriginRow());
            if (null!=col && this.filterColumnString.equalsIgnoreCase(col.trim())) {
                if (logger.isInfoEnabled()) logger.info("Filter Column removing: {}", record.getPk());
                return true;
            }
        }

        if (this.writeTimestampFilterEnabled) {
            // only process rows greater than writeTimeStampFilter
            Long originWriteTimeStamp = record.getPk().getWriteTimestamp();
            if (null==originWriteTimeStamp) {
                return false;
            }
            if (originWriteTimeStamp < minWriteTimeStampFilter
                    || originWriteTimeStamp > maxWriteTimeStampFilter) {
                if (logger.isInfoEnabled()) logger.info("Timestamp filter removing: {}", record.getPk());
                return true;
            }
        }
        return false;
    }

    protected String buildStatement() {
        final StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(PropertyHelper.asString(cqlTable.getColumnNames(true), KnownProperties.PropertyType.STRING_LIST));
        sb.append(" FROM ").append(cqlTable.getKeyspaceTable());
        sb.append(" WHERE ").append(whereBinds());
        return sb.toString();
    }

    private String getFilterColumnString() {
        String rtn = propertyHelper.getString(KnownProperties.FILTER_COLUMN_VALUE);
        if (null!=rtn)
            return rtn.trim();
        return null;
    }

    private Integer getFilterColumnIndex() {
        String filterColumnName = CqlTable.unFormatName(propertyHelper.getAsString(KnownProperties.FILTER_COLUMN_NAME));
        return this.resultColumns.indexOf(filterColumnName);
    }

}
