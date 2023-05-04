package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.schema.ColumnInfo;
import datastax.astra.migrate.schema.TableInfo;
import datastax.astra.migrate.schema.TypeInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected TableInfo tableInfo;
    protected CqlSession sourceSession;
    protected CqlSession astraSession;
    protected List<String> ttlWTCols;
    protected String tsReplaceValStr;
    protected long tsReplaceVal;
    protected long customWriteTime = 0l;
    protected long incrementWriteTime = 0l;

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        this(sourceSession, astraSession, sc, false);
    }

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        if (sourceSession == null) {
            return;
        }

        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        batchSize = Integer.parseInt(Util.getSparkPropOr(sc, "spark.batchSize", "5"));
        fetchSizeInRows = Integer.parseInt(Util.getSparkPropOr(sc, "spark.read.fetch.sizeInRows", "1000"));

        writeLimiter = RateLimiter.create(Integer.parseInt(Util.getSparkPropOr(sc, "spark.writeRateLimit", "40000")));
        maxRetries = Integer.parseInt(sc.get("spark.maxRetries", "0"));
        astraKeyspaceTable = Util.getSparkPropOrEmpty(sc, "spark.target.keyspaceTable");
        if (astraKeyspaceTable.isEmpty()) {
            astraKeyspaceTable = sourceKeyspaceTable;
        }

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Source Keyspace Table: {}", sourceKeyspaceTable);
        logger.info("PARAM -- Source Keyspace: {}", sourceKeyspaceTable.split("\\.")[0]);
        logger.info("PARAM -- Source Table: {}", sourceKeyspaceTable.split("\\.")[1]);
        logger.info("PARAM -- Destination Keyspace: {}", astraKeyspaceTable.split("\\.")[0]);
        logger.info("PARAM -- Destination Table: {}", astraKeyspaceTable.split("\\.")[1]);
        logger.info("PARAM -- ReadRateLimit: {}", readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: {}", writeLimiter.getRate());

        tableInfo = TableInfo.getInstance(sourceSession, sourceKeyspaceTable.split("\\.")[0],
                sourceKeyspaceTable.split("\\.")[1], Util.getSparkPropOrEmpty(sc, "spark.query.origin"));
        String selectCols = String.join(",", tableInfo.getAllColumns());
        String partitionKey = String.join(",", tableInfo.getPartitionKeyColumns());
        ttlWTCols = tableInfo.getTtlAndWriteTimeColumns();
        logger.info("PARAM -- Detected Table Schema: {}", tableInfo);

        writeTimeStampFilter = Boolean
                .parseBoolean(Util.getSparkPropOr(sc, "spark.origin.writeTimeStampFilter", "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
        }

        String minWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, "spark.origin.minWriteTimeStampFilter", "0");
        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, "spark.origin.maxWriteTimeStampFilter", "0");
        if (StringUtils.isNotBlank(maxWriteTimeStampFilterStr)) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        String customWriteTimeStr =
                Util.getSparkPropOr(sc, "spark.target.writeTime.fixedValue", "0");
        if (StringUtils.isNotBlank(customWriteTimeStr) && StringUtils.isNumeric(customWriteTimeStr)) {
            customWriteTime = Long.parseLong(customWriteTimeStr);
        }

        String incrWriteTimeStr =
                Util.getSparkPropOr(sc, "spark.target.writeTime.incrementBy", "0");
        if (StringUtils.isNotBlank(incrWriteTimeStr) && StringUtils.isNumeric(incrWriteTimeStr.trim())) {
            incrementWriteTime = Long.parseLong(incrWriteTimeStr);
        }

        logger.info("PARAM -- TTL-WriteTime Columns: {}", ttlWTCols);
        logger.info("PARAM -- WriteTimes Filter: {}", writeTimeStampFilter);
        logger.info("PARAM -- WriteTime Custom Value: {}", customWriteTime);
        logger.info("PARAM -- WriteTime Increment Value: {}", incrementWriteTime);
        if (writeTimeStampFilter) {
            logger.info("PARAM -- minWriteTimeStampFilter: {} datetime is {}", minWriteTimeStampFilter,
                    Instant.ofEpochMilli(minWriteTimeStampFilter / 1000));
            logger.info("PARAM -- maxWriteTimeStampFilter: {} datetime is {}", maxWriteTimeStampFilter,
                    Instant.ofEpochMilli(maxWriteTimeStampFilter / 1000));
        }

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        ttlWTCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",ttl(" + col + ")");
        });
        ttlWTCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",writetime(" + col + ")");
        });

        String insertCols = Util.getSparkPropOrEmpty(sc, "spark.query.target");
        if (null == insertCols || insertCols.trim().isEmpty()) {
            insertCols = selectCols;
        }
        String insertBinds = String.join(" and ",
                tableInfo.getKeyColumns().stream().map(col -> col + " = ?").collect(Collectors.toList()));

        String originSelectQry;
        if (!isJobMigrateRowsFromFile) {
            originSelectQry = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable +
                    " where token(" + partitionKey + ") >= ? and token(" + partitionKey + ") <= ?  " +
                    sourceSelectCondition + " ALLOW FILTERING";
        } else {
            originSelectQry = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable + " where " + insertBinds;
        }
        logger.info("PARAM -- Origin select query: {}", originSelectQry);
        sourceSelectStatement = sourceSession.prepare(originSelectQry);

        String targetSelectQry = "select " + insertCols + " from " + astraKeyspaceTable + " where " + insertBinds;
        logger.info("PARAM -- Target select query: {}", targetSelectQry);
        astraSelectStatement = astraSession.prepare(targetSelectQry);

        isCounterTable = tableInfo.isCounterTable();
        String fullInsertQuery;
        if (isCounterTable) {
            String updateCols = String.join(" , ",
                    tableInfo.getOtherColumns().stream().map(s -> s + " += ?").collect(Collectors.toList()));
            String updateKeys = String.join(" and ",
                    tableInfo.getKeyColumns().stream().map(s -> s + " = ?").collect(Collectors.toList()));
            fullInsertQuery = "update " + astraKeyspaceTable + " set " + updateCols + " where " + updateKeys;
        } else {
            insertBinds = String.join(" , ", Arrays.stream(insertCols.split(",")).map(col -> " ?").collect(Collectors.toList()));
            fullInsertQuery = "insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")";
            if (!ttlWTCols.isEmpty()) {
                fullInsertQuery += " USING TTL ? AND TIMESTAMP ?";
            }
        }
        logger.info("PARAM -- Target insert query: {}", fullInsertQuery);
        astraInsertStatement = astraSession.prepare(fullInsertQuery);

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        tsReplaceValStr = Util.getSparkPropOr(sc, "spark.target.replace.blankTimestampKeyUsingEpoch", "");
        if (!tsReplaceValStr.isEmpty()) {
            tsReplaceVal = Long.parseLong(tsReplaceValStr);
        }
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        if (isCounterTable) {
            for (int index = 0; index < tableInfo.getNonKeyColumns().size(); index++) {
                TypeInfo typeInfo = tableInfo.getNonKeyColumns().get(index).getTypeInfo();
                int colIdx = tableInfo.getIdColumns().size() + index;
                // compute the counter delta if reading from astra for the difference
                if (astraRow != null) {
                    boundInsertStatement = boundInsertStatement.set(index, (sourceRow.getLong(colIdx) - astraRow.getLong(colIdx)), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, sourceRow.getLong(colIdx), Long.class);
                }
            }
            for (int index = 0; index < tableInfo.getIdColumns().size(); index++) {
                TypeInfo typeInfo = tableInfo.getIdColumns().get(index).getTypeInfo();
                int colIdx = tableInfo.getNonKeyColumns().size() + index;
                boundInsertStatement = boundInsertStatement.set(colIdx, getData(typeInfo, index, sourceRow), typeInfo.getTypeClass());
            }
        } else {
            int index = 0;
            for (index = 0; index < tableInfo.getAllColumns().size(); index++) {
                boundInsertStatement = getBoundStatement(sourceRow, boundInsertStatement, index, tableInfo.getColumns());
                if (boundInsertStatement == null) return null;
            }

            if (!ttlWTCols.isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(sourceRow), Integer.class);
                index++;
                if (customWriteTime > 0) {
                    boundInsertStatement = boundInsertStatement.set(index, customWriteTime, Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getLargestWriteTimeStamp(sourceRow) + incrementWriteTime, Long.class);
                }
            }
        }

        // Batch insert for large records may take longer, hence 10 secs to avoid timeout errors
        return boundInsertStatement.setTimeout(Duration.ofSeconds(10));
    }

    public int getLargestTTL(Row sourceRow) {
        return IntStream.range(0, ttlWTCols.size())
                .map(i -> sourceRow.getInt(tableInfo.getAllColumns().size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        return IntStream.range(0, ttlWTCols.size())
                .mapToLong(i -> sourceRow.getLong(tableInfo.getAllColumns().size() + ttlWTCols.size() + i)).max().getAsLong();
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < tableInfo.getKeyColumns().size(); index++) {
            boundSelectStatement = getBoundStatement(sourceRow, boundSelectStatement, index, tableInfo.getIdColumns());
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private BoundStatement getBoundStatement(Row sourceRow, BoundStatement boundSelectStatement, int index,
                                             List<ColumnInfo> cols) {
        TypeInfo typeInfo = cols.get(index).getTypeInfo();
        Object colData = getData(typeInfo, index, sourceRow);

        // Handle rows with blank values in primary-key fields
        if (index < tableInfo.getKeyColumns().size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, typeInfo.getTypeClass(), sourceRow);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, typeInfo.getTypeClass());
        return boundSelectStatement;
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow) {
        return handleBlankInPrimaryKey(index, colData, dataType, sourceRow, true);
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < tableInfo.getKeyColumns().size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        getKey(sourceRow, tableInfo), tableInfo.getKeyColumns().get(index));
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < tableInfo.getKeyColumns().size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(sourceRow, tableInfo), tableInfo.getKeyColumns().get(index));
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(sourceRow, tableInfo), tableInfo.getKeyColumns().get(index), Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

}
