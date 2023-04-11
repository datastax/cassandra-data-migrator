package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.Record;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public abstract class AbstractOriginSelectStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final List<Integer> writeTimestampIndexes = new ArrayList<>();
    protected final List<Integer> ttlIndexes = new ArrayList<>();

    private final Boolean writeTimestampFilterEnabled;
    private final Long minWriteTimeStampFilter;
    private final Long maxWriteTimeStampFilter;

    private final Boolean filterColumnEnabled;
    private final Integer filterColumnIndex;
    private final MigrateDataType filterColumnType;
    private final String filterColumnString;

    public AbstractOriginSelectStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);
        this.session = cqlHelper.getOriginSession();

        resultColumns.addAll(propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES));
        resultTypes.addAll(propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES));

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

    public Long getLargestWriteTimeStamp(Row row) {
        if (null==writeTimestampIndexes || writeTimestampIndexes.isEmpty()) return null;
        OptionalLong max = writeTimestampIndexes.stream()
                .mapToLong(row::getLong)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsLong() : null;
    }

    public Integer getLargestTTL(Row row) {
        if (null==ttlIndexes || ttlIndexes.isEmpty()) return null;
        OptionalInt max = ttlIndexes.stream()
                .mapToInt(row::getInt)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsInt() : null;
    }

    private String ttlAndWritetimeCols() {
        StringBuilder sb = new StringBuilder();
        if (null != cqlHelper.getTtlCols()) {
            cqlHelper.getTtlCols().forEach(col -> {
                String ttlCol = resultColumns.get(col);
                String cleanCol = ttlCol.replaceAll("-", "_").replace("\"", "");
                sb.append(",TTL(").append(ttlCol).append(") as ttl_").append(cleanCol);
                resultColumns.add("ttl_" + cleanCol);
                resultTypes.add(new MigrateDataType("1")); // int
                ttlIndexes.add(resultColumns.size()-1);
            });
        }
        if (null != cqlHelper.getWriteTimeStampCols()) {
            cqlHelper.getWriteTimeStampCols().forEach(col -> {
                String wtCol = resultColumns.get(col);
                String cleanCol = wtCol.replaceAll("-", "_").replace("\"", "");
                sb.append(",WRITETIME(").append(wtCol).append(") as writetime_").append(cleanCol);
                resultColumns.add("writetime_" + cleanCol);
                resultTypes.add(new MigrateDataType("2")); // application using as <long>, though Cassandra uses <timestamp>
                writeTimestampIndexes.add(resultColumns.size()-1);
            });
        }
        return sb.toString();
    }

    protected String buildStatement() {
        final StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(PropertyHelper.asString(this.resultColumns, KnownProperties.PropertyType.STRING_LIST)).append(ttlAndWritetimeCols());
        sb.append(" FROM ").append(propertyHelper.getAsString(KnownProperties.ORIGIN_KEYSPACE_TABLE));
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
        Long min = getMinWriteTimeStampFilter();
        Long max = getMaxWriteTimeStampFilter();
        List<Integer> writetimeCols = cqlHelper.getWriteTimeStampCols();
        return (null != min && null != max &&
                min > 0 && max > 0 && min < max &&
                null != writetimeCols && !writetimeCols.isEmpty());
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
