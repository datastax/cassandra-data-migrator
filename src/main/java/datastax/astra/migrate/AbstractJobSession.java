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

        batchSize = propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
        fetchSizeInRows = propertyHelper.getInteger(KnownProperties.READ_FETCH_SIZE);

        printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
        if (!meetsMinimum(KnownProperties.SPARK_STATS_AFTER, printStatsAfter, 1)) {
            logger.warn(KnownProperties.SPARK_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.SPARK_STATS_AFTER, KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
        }

        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_READ));
        writeLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_WRITE));
        maxRetries = propertyHelper.getInteger(KnownProperties.SPARK_MAX_RETRIES);

        sourceKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
        targetKeyspaceTable = propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);

        ttlCols = propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_COLS);
        writeTimeStampCols = propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_COLS);

        writeTimeStampFilter = propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_WRITETS_ENABLED);
        if (writeTimeStampFilter) {
            batchSize = 1;
            propertyHelper.setProperty(KnownProperties.SPARK_BATCH_SIZE, batchSize);
        }
        minWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MIN);
        maxWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MAX);

        customWritetime = propertyHelper.getLong(KnownProperties.TARGET_CUSTOM_WRITETIME);

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Source Keyspace Table: {}", sourceKeyspaceTable);
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

        String selectCols = String.join(",", propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES));
        String partitionKey = String.join(",", propertyHelper.getStringList(KnownProperties.ORIGIN_PARTITION_KEY));
        String sourceSelectCondition = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_CONDITION);
        if (null != sourceSelectCondition && !sourceSelectCondition.isEmpty() && !sourceSelectCondition.trim().toUpperCase().startsWith("AND")) {
            sourceSelectCondition = " AND " + sourceSelectCondition;
            propertyHelper.setProperty(KnownProperties.ORIGIN_FILTER_CONDITION, sourceSelectCondition);
        }

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        allCols = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);
        if (null != ttlCols) {
            ttlCols.forEach(col -> {
                selectTTLWriteTimeCols.append(",ttl(" + allCols.get(col) + ")");
            });
        }
        if (null != writeTimeStampCols) {
            writeTimeStampCols.forEach(col -> {
                selectTTLWriteTimeCols.append(",writetime(" + allCols.get(col) + ")");
            });
        }
        selectColTypes = propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES);
        String idCols = String.join(",", propertyHelper.getStringList(KnownProperties.TARGET_PRIMARY_KEY));
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String insertCols = String.join(",", propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES));
        if (null == insertCols || insertCols.trim().isEmpty()) {
            insertCols = selectCols;
            propertyHelper.setProperty(KnownProperties.TARGET_COLUMN_NAMES, propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES));
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
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable +
                    " where token(" + partitionKey.trim() + ") >= ? and token(" + partitionKey.trim() + ") <= ?  " +
                    sourceSelectCondition + " ALLOW FILTERING";
        } else {
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable + " where " + insertBinds;
        }
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", fullSelectQuery);
        sourceSelectStatement = sourceSession.prepare(fullSelectQuery);

        astraSelectStatement = astraSession.prepare(
                "select " + insertCols + " from " + targetKeyspaceTable
                        + " where " + insertBinds);

        hasRandomPartitioner = propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
        if (isCounterTable) {
            updateSelectMapping = propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEX);
            logger.info("PARAM -- TARGET INSERT Query used: {}", KnownProperties.ORIGIN_COUNTER_CQL);
            astraInsertStatement = astraSession.prepare(propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_CQL));
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
            if (null != ttlCols && !ttlCols.isEmpty()) {
                fullInsertQuery += " USING TTL ?";
                if (null != writeTimeStampCols &&  !writeTimeStampCols.isEmpty()) {
                    fullInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (null != writeTimeStampCols &&  !writeTimeStampCols.isEmpty()) {
                fullInsertQuery += " USING TIMESTAMP ?";
            }
            logger.info("PARAM -- TARGET INSERT Query used: {}", fullInsertQuery);
            astraInsertStatement = astraSession.prepare(fullInsertQuery);
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (null != propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS))
            tsReplaceVal = propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS);
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        if (isCounterTable) {
            for (int index = 0; index < selectColTypes.size(); index++) {
                MigrateDataType dataType = selectColTypes.get(updateSelectMapping.get(index));
                // compute the counter delta if reading from astra for the difference
                if (astraRow != null && index < (selectColTypes.size() - idColTypes.size())) {
                    boundInsertStatement = boundInsertStatement.set(index, (sourceRow.getLong(updateSelectMapping.get(index)) - astraRow.getLong(updateSelectMapping.get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, updateSelectMapping.get(index), sourceRow), dataType.typeClass);
                }
            }
        } else {
            int index = 0;
            for (index = 0; index < selectColTypes.size(); index++) {
                boundInsertStatement = getBoundStatement(sourceRow, boundInsertStatement, index, selectColTypes);
                if (boundInsertStatement == null) return null;
            }

            if (null != ttlCols && !ttlCols.isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(sourceRow), Integer.class);
                index++;
            }
            if (null != writeTimeStampCols && !writeTimeStampCols.isEmpty()) {
                if (customWritetime > 0) {
                    boundInsertStatement = boundInsertStatement.set(index, customWritetime, Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getLargestWriteTimeStamp(sourceRow), Long.class);
                }
            }
        }

        // Batch insert for large records may take longer, hence 10 secs to avoid timeout errors
        return boundInsertStatement.setTimeout(Duration.ofSeconds(10));
    }

    public int getLargestTTL(Row sourceRow) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> sourceRow.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> sourceRow.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < idColTypes.size(); index++) {
            boundSelectStatement = getBoundStatement(sourceRow, boundSelectStatement, index, idColTypes);
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private BoundStatement getBoundStatement(Row sourceRow, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = getData(dataTypeObj, index, sourceRow);

        // Handle rows with blank values in primary-key fields
        if (index < idColTypes.size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, dataTypeObj.typeClass, sourceRow);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, dataTypeObj.typeClass);
        return boundSelectStatement;
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow) {
        return handleBlankInPrimaryKey(index, colData, dataType, sourceRow, true);
    }

    protected Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        getKey(sourceRow), allCols.get(index));
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(sourceRow), allCols.get(index));
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(sourceRow), allCols.get(index), Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

    private boolean meetsMinimum(String valueName, Integer testValue, Integer minimumValue) {
        if (null != minimumValue && null != testValue && testValue >= minimumValue)
            return true;
        logger.warn(valueName + " must be greater than or equal to " + minimumValue + ".  Current value does not meet this requirement: " + testValue);
        return false;
    }
}
