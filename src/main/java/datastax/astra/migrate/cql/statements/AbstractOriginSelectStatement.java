package datastax.astra.migrate.cql.statements;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.cql.Record;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractOriginSelectStatement extends BaseCdmStatement {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected final List<Integer> writeTimestampIndexes = new ArrayList<>();
    protected final List<Integer> ttlIndexes = new ArrayList<>();

    public AbstractOriginSelectStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        this.statement = buildStatement();
        this.session = cqlHelper.getOriginSession();

        resultColumns.addAll(propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES));
        resultTypes.addAll(propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES));
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

        if (cqlHelper.hasFilterColumn()) {
            String col = (String) cqlHelper.getData(cqlHelper.getFilterColType(), cqlHelper.getFilterColIndex(), record.getOriginRow());
            if (col.trim().equalsIgnoreCase(cqlHelper.getFilterColValue())) {
                if (logger.isInfoEnabled()) logger.info("Filter Column removing: {}", record.getPk());
                return true;
            }
        }

        if (cqlHelper.hasWriteTimestampFilter()) {
            // only process rows greater than writeTimeStampFilter
            Long originWriteTimeStamp = record.getPk().getWriteTimestamp();
            if (originWriteTimeStamp < cqlHelper.getMinWriteTimeStampFilter()
                    || originWriteTimeStamp > cqlHelper.getMaxWriteTimeStampFilter()) {
                if (logger.isInfoEnabled()) logger.info("Timestamp filter removing: {}", record.getPk());
                return true;
            }
        }
        return false;
    }

    public Long getLargestWriteTimeStamp(Row row) {
        if (null==writeTimestampIndexes || writeTimestampIndexes.isEmpty()) return null;
        return writeTimestampIndexes.stream().mapToLong(row::getLong).max().getAsLong();
    }

    public Long getLargestTTL(Row row) {
        if (null==ttlIndexes || ttlIndexes.isEmpty()) return null;
        return ttlIndexes.stream().mapToLong(row::getLong).max().getAsLong();
    }

    private String ttlAndWritetimeCols() {
        StringBuilder sb = new StringBuilder();
        if (null != cqlHelper.getTtlCols()) {
            cqlHelper.getTtlCols().forEach(col -> {
                sb.append(",TTL(" + col + ") as ttl_" + col);
                resultColumns.add("ttl_" + col);
                resultTypes.add(new MigrateDataType()); // unknown
                ttlIndexes.add(resultColumns.size()-1);
            });
        }
        if (null != cqlHelper.getWriteTimeStampCols()) {
            cqlHelper.getWriteTimeStampCols().forEach(col -> {
                sb.append(",WRITETIME(" + col + ") as writetime_" + col);
                resultColumns.add("writetime_" + col);
                resultTypes.add(new MigrateDataType("2")); // application using as <long>, though Cassandra uses <timestamp>
                writeTimestampIndexes.add(resultColumns.size()-1);
            });
        }
        return sb.toString();
    }

    protected String buildStatement() {
        final StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_NAMES)).append(ttlAndWritetimeCols());
        sb.append(" FROM ").append(propertyHelper.getAsString(KnownProperties.ORIGIN_KEYSPACE_TABLE));
        sb.append(" WHERE ").append(whereBinds());
        return sb.toString();
    }

}
