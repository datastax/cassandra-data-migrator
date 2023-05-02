package com.datastax.cdm.cql.statement;

import com.datastax.cdm.feature.WritetimeTTLColumn;
import com.datastax.cdm.properties.ColumnsKeysTypes;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.job.MigrateDataType;
import com.datastax.cdm.cql.CqlHelper;
import com.datastax.cdm.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public abstract class AbstractOriginSelectStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final WritetimeTTLColumn writetimeTTLColumnFeature;

    private final Boolean writeTimestampFilterEnabled;
    private final Long minWriteTimeStampFilter;
    private final Long maxWriteTimeStampFilter;

    private final Boolean filterColumnEnabled;
    private final Integer filterColumnIndex;
    private final MigrateDataType filterColumnType;
    private final String filterColumnString;

    public AbstractOriginSelectStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper, CqlSession session) {
        super(propertyHelper, cqlHelper, session);

        resultColumns.addAll(ColumnsKeysTypes.getOriginColumnNames(propertyHelper));
        resultTypes.addAll(ColumnsKeysTypes.getOriginColumnTypes(propertyHelper));

        writetimeTTLColumnFeature = (WritetimeTTLColumn) cqlHelper.getFeature(Featureset.WRITETIME_TTL_COLUMN);

        minWriteTimeStampFilter = getMinWriteTimeStampFilter();
        maxWriteTimeStampFilter = getMaxWriteTimeStampFilter();
        writeTimestampFilterEnabled = hasWriteTimestampFilter();
        if (writeTimestampFilterEnabled) {
            logger.info("PARAM -- {}: {} datetime is {}", KnownProperties.FILTER_WRITETS_MIN, getMinWriteTimeStampFilter(),
                    Instant.ofEpochMilli(getMinWriteTimeStampFilter() / 1000));
            logger.info("PARAM -- {}: {} datetime is {}", KnownProperties.FILTER_WRITETS_MAX, getMaxWriteTimeStampFilter(),
                    Instant.ofEpochMilli(getMaxWriteTimeStampFilter() / 1000));
        }

        filterColumnString = getFilterColumnString();
        filterColumnIndex = getFilterColumnIndex();
        filterColumnType = getFilterColumnType();
        filterColumnEnabled = (null != filterColumnIndex && null != filterColumnType && null != filterColumnString && !filterColumnString.isEmpty());
        if (filterColumnEnabled) {
            logger.info("PARAM -- {}: {} ", KnownProperties.FILTER_COLUMN_NAME, filterColumnString);
            logger.info("PARAM -- {}: {} ", KnownProperties.FILTER_COLUMN_VALUE, filterColumnString);
        }

        this.statement = buildStatement();
    }

    public ResultSet execute(BoundStatement boundStatement) {
        return session.execute(boundStatement);
    };

    public boolean isRecordValid(Record record) {
        if (null == record) {
            throw new RuntimeException("Record is null");
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
            String col = (String) cqlHelper.getData(this.filterColumnType, this.filterColumnIndex, record.getOriginRow());
            if (this.filterColumnString.equalsIgnoreCase(col.trim())) {
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

    private String ttlAndWritetimeCols() {
        if (!writetimeTTLColumnFeature.isEnabled()) {
            return "";
        }

        List<Integer> ttlCols = writetimeTTLColumnFeature.getIntegerList(WritetimeTTLColumn.Property.TTL_INDEXES);
        List<Integer> wtCols = writetimeTTLColumnFeature.getIntegerList(WritetimeTTLColumn.Property.WRITETIME_INDEXES);

        StringBuilder sb = new StringBuilder();
        if (null != ttlCols) {
            ttlCols.forEach(col -> {
                String ttlCol = resultColumns.get(col);
                String cleanCol = ttlCol.replaceAll("-", "_").replace("\"", "");
                sb.append(",TTL(").append(ttlCol).append(") as ttl_").append(cleanCol);
                resultColumns.add("ttl_" + cleanCol);
                resultTypes.add(new MigrateDataType("1")); // int
                writetimeTTLColumnFeature.addTTLSelectColumnIndex(resultColumns.size()-1);
            });
        }

        if (null != wtCols) {
            wtCols.forEach(col -> {
                String wtCol = resultColumns.get(col);
                String cleanCol = wtCol.replaceAll("-", "_").replace("\"", "");
                sb.append(",WRITETIME(").append(wtCol).append(") as writetime_").append(cleanCol);
                resultColumns.add("writetime_" + cleanCol);
                resultTypes.add(new MigrateDataType("2")); // application using as <long>, though Cassandra uses <timestamp>
                writetimeTTLColumnFeature.addWritetimeSelectColumnIndex(resultColumns.size()-1);
            });
        }
        return sb.toString();
    }

    protected String buildStatement() {
        final StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(PropertyHelper.asString(this.resultColumns, KnownProperties.PropertyType.STRING_LIST)).append(ttlAndWritetimeCols());
        sb.append(" FROM ").append(ColumnsKeysTypes.getOriginKeyspaceTable(propertyHelper));
        sb.append(" WHERE ").append(whereBinds());
        return sb.toString();
    }

    private Long getMinWriteTimeStampFilter() {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN);
    }
    private Long getMaxWriteTimeStampFilter() {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX);
    }
    public boolean hasWriteTimestampFilter() {
        if (!writetimeTTLColumnFeature.isEnabled()) return false;
        if (writetimeTTLColumnFeature.getCustomWritetime() <= 0L &&
                null == writetimeTTLColumnFeature.getIntegerList(WritetimeTTLColumn.Property.WRITETIME_INDEXES)) return false;

        Long min = getMinWriteTimeStampFilter();
        Long max = getMaxWriteTimeStampFilter();
        return (null != min && null != max &&
                min > 0 && max > 0 && min < max);
    }

    private String getFilterColumnString() {
        String rtn = propertyHelper.getString(KnownProperties.FILTER_COLUMN_VALUE);
        if (null!=rtn) return rtn.trim();
        return null;
    }
    private Integer getFilterColumnIndex() {
        String filterColumnName = propertyHelper.getAsString(KnownProperties.FILTER_COLUMN_NAME);
        if (null==filterColumnName || null==this.resultColumns || this.resultColumns.isEmpty()) {
            return null;
        }
        return this.resultColumns.indexOf(filterColumnName);
    }
    private MigrateDataType getFilterColumnType() {
        if (null==filterColumnIndex || this.filterColumnIndex < 0 || null==this.resultTypes || this.resultTypes.isEmpty()) {
            return null;
        }
        return this.resultTypes.get(filterColumnIndex);
    }

}
