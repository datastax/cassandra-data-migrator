package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.properties.KnownProperties;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        this(originSession, targetSession, sc, false);
    }

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        if (originSession == null) {
            return;
        }

        this.originSessionSession = originSession;
        this.targetSession = targetSession;

        batchSize = propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
        fetchSizeInRows = propertyHelper.getInteger(KnownProperties.READ_FETCH_SIZE);
        printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
        if (!propertyHelper.meetsMinimum(KnownProperties.SPARK_STATS_AFTER, printStatsAfter, 1)) {
            logger.warn(KnownProperties.SPARK_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.SPARK_STATS_AFTER, KnownProperties.getDefault(KnownProperties.SPARK_STATS_AFTER));
            printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
        }

        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_READ));
        writeLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_WRITE));
        maxRetries = propertyHelper.getInteger(KnownProperties.SPARK_MAX_RETRIES);

        originKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
        targetKeyspaceTable = propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);

        String ttlColsStr = propertyHelper.getAsString(KnownProperties.ORIGIN_TTL_COLS);
        if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
            for (String ttlCol : ttlColsStr.split(",")) {
                ttlCols.add(Integer.parseInt(ttlCol));
            }
        }

        String writeTimestampColsStr = propertyHelper.getAsString(KnownProperties.ORIGIN_WRITETIME_COLS);
        if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
            for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
            }
        }

        writeTimeStampFilter = propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_WRITETS_ENABLED);
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
        }

        minWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MIN);
        maxWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MAX);
        customWritetime = propertyHelper.getLong(KnownProperties.TARGET_CUSTOM_WRITETIME);

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Source Keyspace Table: {}", originKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: {}", targetKeyspaceTable);
        logger.info("PARAM -- ReadRateLimit: {}", readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: {}", writeLimiter.getRate());
        logger.info("PARAM -- TTLCols: {}", ttlCols);
        logger.info("PARAM -- WriteTimestampFilterCols: {}", writeTimeStampCols);
        logger.info("PARAM -- WriteTimestampFilter: {}", writeTimeStampFilter);
        if (writeTimeStampFilter) {
            logger.info("PARAM -- minWriteTimeStampFilter: {} datetime is {}", minWriteTimeStampFilter,
                    Instant.ofEpochMilli(minWriteTimeStampFilter / 1000));
            logger.info("PARAM -- maxWriteTimeStampFilter: {} datetime is {}", maxWriteTimeStampFilter,
                    Instant.ofEpochMilli(maxWriteTimeStampFilter / 1000));
        }

        String selectCols = propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_NAMES);
        String partitionKey = propertyHelper.getAsString(KnownProperties.ORIGIN_PARTITION_KEY);
        String originSelectCondition = propertyHelper.getAsString(KnownProperties.ORIGIN_FILTER_CONDITION);
        if (!originSelectCondition.isEmpty() && !originSelectCondition.trim().toUpperCase().startsWith("AND")) {
            originSelectCondition = " AND " + originSelectCondition;
        }

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        allCols = selectCols.split(",");
        ttlCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",ttl(" + allCols[col] + ")");
        });
        writeTimeStampCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",writetime(" + allCols[col] + ")");
        });
        selectColTypes = getTypes(propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_TYPES));
        String idCols = propertyHelper.getAsString(KnownProperties.TARGET_PRIMARY_KEY);
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String insertCols = propertyHelper.getAsString(KnownProperties.TARGET_COLUMN_NAMES);
        if (null == insertCols || insertCols.trim().isEmpty()) {
            insertCols = selectCols;
        }
        String insertBinds = "";
        for (String str : idCols.split(",")) {
            if (insertBinds.isEmpty()) {
                insertBinds = str + "= ?";
            } else {
                insertBinds += " and " + str + "= ?";
            }
        }

        String fullSelectQuery;
        if (!isJobMigrateRowsFromFile) {
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + originKeyspaceTable +
                    " where token(" + partitionKey.trim() + ") >= ? and token(" + partitionKey.trim() + ") <= ?  " +
                    originSelectCondition + " ALLOW FILTERING";
        } else {
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + originKeyspaceTable + " where " + insertBinds;
        }
        originSelectStatement = originSession.prepare(fullSelectQuery);
        logger.info("PARAM -- Query used: {}", fullSelectQuery);

        targetSelectStatement = targetSession.prepare(
                "select " + insertCols + " from " + targetKeyspaceTable
                        + " where " + insertBinds);

        hasRandomPartitioner = propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
        if (isCounterTable) {
            String updateSelectMappingStr = propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_INDEXES);
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_CQL);
            targetInsertStatement = targetSession.prepare(counterTableUpdate);
        } else {
            insertBinds = "";
            for (String str : insertCols.split(",")) {
                if (insertBinds.isEmpty()) {
                    insertBinds += "?";
                } else {
                    insertBinds += ", ?";
                }
            }

            String fullInsertQuery = "insert into " + targetKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")";
            if (!ttlCols.isEmpty()) {
                fullInsertQuery += " USING TTL ?";
                if (!writeTimeStampCols.isEmpty()) {
                    fullInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (!writeTimeStampCols.isEmpty()) {
                fullInsertQuery += " USING TIMESTAMP ?";
            }
            targetInsertStatement = targetSession.prepare(fullInsertQuery);
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        tsReplaceValStr = propertyHelper.getAsString(KnownProperties.TARGET_REPLACE_MISSING_TS);
        if (!tsReplaceValStr.isEmpty()) {
            tsReplaceVal = Long.parseLong(tsReplaceValStr);
        }
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row originRow, Row targetRow) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        if (isCounterTable) {
            for (int index = 0; index < selectColTypes.size(); index++) {
                MigrateDataType dataType = selectColTypes.get(updateSelectMapping.get(index));
                // compute the counter delta if reading from target for the difference
                if (targetRow != null && index < (selectColTypes.size() - idColTypes.size())) {
                    boundInsertStatement = boundInsertStatement.set(index, (originRow.getLong(updateSelectMapping.get(index)) - targetRow.getLong(updateSelectMapping.get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, updateSelectMapping.get(index), originRow), dataType.typeClass);
                }
            }
        } else {
            int index = 0;
            for (index = 0; index < selectColTypes.size(); index++) {
                boundInsertStatement = getBoundStatement(originRow, boundInsertStatement, index, selectColTypes);
                if (boundInsertStatement == null) return null;
            }

            if (!ttlCols.isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(originRow), Integer.class);
                index++;
            }
            if (!writeTimeStampCols.isEmpty()) {
                if (customWritetime > 0) {
                    boundInsertStatement = boundInsertStatement.set(index, customWritetime, Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getLargestWriteTimeStamp(originRow), Long.class);
                }
            }
        }

        // Batch insert for large records may take longer, hence 10 secs to avoid timeout errors
        return boundInsertStatement.setTimeout(Duration.ofSeconds(10));
    }

    public int getLargestTTL(Row row) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> row.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row row) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> row.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
    }

    public BoundStatement selectFromTarget(PreparedStatement selectStatement, Row originRow) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < idColTypes.size(); index++) {
            boundSelectStatement = getBoundStatement(originRow, boundSelectStatement, index, idColTypes);
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private BoundStatement getBoundStatement(Row row, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = getData(dataTypeObj, index, row);

        // Handle rows with blank values in primary-key fields
        if (index < idColTypes.size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, dataTypeObj.typeClass, row);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, dataTypeObj.typeClass);
        return boundSelectStatement;
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row originRow) {
        return handleBlankInPrimaryKey(index, colData, dataType, originRow, true);
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row originRow, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        getKey(originRow), allCols[index]);
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf "+KnownProperties.TARGET_REPLACE_MISSING_TS+"\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(originRow), allCols[index]);
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(originRow), allCols[index], Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

}
