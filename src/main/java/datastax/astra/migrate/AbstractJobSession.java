package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.properties.KnownProperties;
import org.apache.commons.lang.StringUtils;
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

        batchSize = new Integer(Util.getSparkPropOr(sc, KnownProperties.SPARK_BATCH_SIZE, "5"));
        fetchSizeInRows = new Integer(Util.getSparkPropOr(sc, KnownProperties.READ_FETCH_SIZE, "1000"));
        printStatsAfter = new Integer(Util.getSparkPropOr(sc, KnownProperties.SPARK_STATS_AFTER, "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, KnownProperties.SPARK_LIMIT_READ, "20000")));
        writeLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, KnownProperties.SPARK_LIMIT_WRITE, "40000")));
        maxRetries = Integer.parseInt(sc.get(KnownProperties.SPARK_MAX_RETRIES, "0"));

        originKeyspaceTable = Util.getSparkProp(sc, KnownProperties.ORIGIN_KEYSPACE_TABLE);
        targetKeyspaceTable = Util.getSparkProp(sc, KnownProperties.TARGET_KEYSPACE_TABLE);

        String ttlColsStr = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TTL_COLS);
        if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
            for (String ttlCol : ttlColsStr.split(",")) {
                ttlCols.add(Integer.parseInt(ttlCol));
            }
        }

        String writeTimestampColsStr = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_WRITETIME_COLS);
        if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
            for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
            }
        }

        writeTimeStampFilter = Boolean
                .parseBoolean(Util.getSparkPropOr(sc, KnownProperties.ORIGIN_FILTER_WRITETS_ENABLED, "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
        }

        String minWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, KnownProperties.ORIGIN_FILTER_WRITETS_MIN, "0");
        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, KnownProperties.ORIGIN_FILTER_WRITETS_MAX, "0");
        if (null != maxWriteTimeStampFilterStr && maxWriteTimeStampFilterStr.trim().length() > 1) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        String customWriteTimeStr =
                Util.getSparkPropOr(sc, KnownProperties.TARGET_CUSTOM_WRITETIME, "0");
        if (null != customWriteTimeStr && customWriteTimeStr.trim().length() > 1 && StringUtils.isNumeric(customWriteTimeStr.trim())) {
            customWritetime = Long.parseLong(customWriteTimeStr);
        }

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

        String selectCols = Util.getSparkProp(sc, KnownProperties.ORIGIN_COLUMN_NAMES);
        String partitionKey = Util.getSparkProp(sc, KnownProperties.ORIGIN_PARTITION_KEY);
        String originSelectCondition = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_FILTER_CONDITION);
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
        selectColTypes = getTypes(Util.getSparkProp(sc, KnownProperties.ORIGIN_COLUMN_TYPES));
        String idCols = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_PRIMARY_KEY);
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String insertCols = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_COLUMN_NAMES);
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

        hasRandomPartitioner = Boolean.parseBoolean(Util.getSparkPropOr(sc, KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER, "false"));
        isCounterTable = Boolean.parseBoolean(Util.getSparkPropOr(sc, KnownProperties.ORIGIN_IS_COUNTER, "false"));
        if (isCounterTable) {
            String updateSelectMappingStr = Util.getSparkPropOr(sc, KnownProperties.ORIGIN_COUNTER_INDEXES, "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = Util.getSparkProp(sc, KnownProperties.ORIGIN_COUNTER_CQL);
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
        tsReplaceValStr = Util.getSparkPropOr(sc, KnownProperties.TARGET_REPLACE_MISSING_TS, "");
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
