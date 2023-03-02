package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        this(sourceSession, astraSession, sc, false);
    }

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        boolean validBootstrap = true;

        if (sourceSession == null) {
            return;
        }

        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        batchSize = new Integer(Util.getSparkPropOr(sc, "spark.batchSize", "5"));
        fetchSizeInRows = new Integer(Util.getSparkPropOr(sc, "spark.read.fetch.sizeInRows", "1000"));
        printStatsAfter = new Integer(Util.getSparkPropOr(sc, "spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, "spark.readRateLimit", "20000")));
        writeLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, "spark.writeRateLimit", "40000")));
        maxRetries = Integer.parseInt(sc.get("spark.maxRetries", "0"));

        sourceKeyspaceTable = Util.getSparkProp(sc, "spark.origin.keyspaceTable");
        astraKeyspaceTable = Util.getSparkProp(sc, "spark.target.keyspaceTable");

        String ttlColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.ttl.cols");
        if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
            for (String ttlCol : ttlColsStr.split(",")) {
                ttlCols.add(Integer.parseInt(ttlCol));
            }
        }

        String writeTimestampColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.writetime.cols");
        if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
            for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
            }
        }

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
        if (null != maxWriteTimeStampFilterStr && maxWriteTimeStampFilterStr.trim().length() > 1) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        String customWriteTimeStr =
                Util.getSparkPropOr(sc, "spark.target.custom.writeTime", "0");
        if (null != customWriteTimeStr && customWriteTimeStr.trim().length() > 1 && StringUtils.isNumeric(customWriteTimeStr.trim())) {
            customWritetime = Long.parseLong(customWriteTimeStr);
        }

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Source Keyspace Table: {}", sourceKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: {}", astraKeyspaceTable);
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

        String selectCols = Util.getSparkProp(sc, "spark.query.origin");
        String partitionKey = Util.getSparkProp(sc, "spark.query.origin.partitionKey");
        String sourceSelectCondition = Util.getSparkPropOrEmpty(sc, "spark.query.condition");
        if (!sourceSelectCondition.isEmpty() && !sourceSelectCondition.trim().toUpperCase().startsWith("AND")) {
            sourceSelectCondition = " AND " + sourceSelectCondition;
        }

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        allCols = selectCols.split(",");
        ttlCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",ttl(" + allCols[col] + ")");
        });
        writeTimeStampCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",writetime(" + allCols[col] + ")");
        });
        selectColTypes = getTypes(Util.getSparkProp(sc, "spark.query.types"));
        String idCols = Util.getSparkPropOrEmpty(sc, "spark.query.target.id");
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String explodeColumnProperty = "spark.query.origin.explodeColumn";
        String explodeColumnString = Util.getSparkPropOrEmpty(sc, explodeColumnProperty);
        if (null != explodeColumnString && !explodeColumnString.trim().isEmpty()) {
            int explodeColumn = Integer.parseInt(explodeColumnString);
            if (selectColTypes.get(explodeColumn).typeClass.equals(Map.class)) {
                mapToExpandIndex = explodeColumn;
                mapToExpandKeyType = selectColTypes.get(explodeColumn).subTypes.get(0);
                mapToExpandKeyMigrateDataType = new MigrateDataType(selectColTypes.get(explodeColumn).getDataSubType(0));
                mapToExpandValueType = selectColTypes.get(explodeColumn).subTypes.get(1);
                mapToExpandValueMigrateDataType = new MigrateDataType(selectColTypes.get(explodeColumn).getDataSubType(1));
            } else {
                logger.error("{} is not a Map type.  Column: {} Type: {}", explodeColumnProperty, explodeColumn, selectColTypes.get(explodeColumn).typeClass);
                validBootstrap = false;
            }
        }

        String insertCols = Util.getSparkPropOrEmpty(sc, "spark.query.target");
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
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable +
                    " where token(" + partitionKey.trim() + ") >= ? and token(" + partitionKey.trim() + ") <= ?  " +
                    sourceSelectCondition + " ALLOW FILTERING";
        } else {
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable + " where " + insertBinds;
        }
        sourceSelectStatement = sourceSession.prepare(fullSelectQuery);
        logger.info("PARAM -- Query used: {}", fullSelectQuery);

        astraSelectStatement = astraSession.prepare(
                "select " + insertCols + " from " + astraKeyspaceTable
                        + " where " + insertBinds);

        hasRandomPartitioner = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.origin.hasRandomPartitioner", "false"));
        isCounterTable = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.counterTable", "false"));
        if (isCounterTable) {
            String updateSelectMappingStr = Util.getSparkPropOr(sc, "spark.counterTable.cql.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = Util.getSparkProp(sc, "spark.counterTable.cql");
            astraInsertStatement = astraSession.prepare(counterTableUpdate);
        } else {
            insertBinds = "";
            for (String str : insertCols.split(",")) {
                if (insertBinds.isEmpty()) {
                    insertBinds += "?";
                } else {
                    insertBinds += ", ?";
                }
            }

            String fullInsertQuery = "insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")";
            if (!ttlCols.isEmpty()) {
                fullInsertQuery += " USING TTL ?";
                if (!writeTimeStampCols.isEmpty()) {
                    fullInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (!writeTimeStampCols.isEmpty()) {
                fullInsertQuery += " USING TIMESTAMP ?";
            }
            astraInsertStatement = astraSession.prepare(fullInsertQuery);
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        tsReplaceValStr = Util.getSparkPropOr(sc, "spark.target.replace.blankTimestampKeyUsingEpoch", "");
        if (!tsReplaceValStr.isEmpty()) {
            tsReplaceVal = Long.parseLong(tsReplaceValStr);
        }

        if (!validBootstrap) {
            System.out.println("Bootstrap failed - check logs.  Exiting.");
            System.exit(1);
        }
    }

    public BoundStatement bindInsertOneToOne(PreparedStatement insertStatement, Row sourceRow, Row astraRow, Object mapKey, Object mapValue) {
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
            int columnCount = selectColTypes.size();
            for (index = 0; index < columnCount; index++) {
                if (null!=mapKey && mapToExpandIndex == index) {
                    boundInsertStatement = boundInsertStatement.set(index, mapKey, mapToExpandKeyType);
                    index++;
                    boundInsertStatement = boundInsertStatement.set(index, mapValue, mapToExpandValueType);
                    columnCount++;
                }
                else {
                    boundInsertStatement = getBoundStatement(sourceRow, boundInsertStatement, index, selectColTypes);
                }
                if (boundInsertStatement == null) return null;
            }

            if (!ttlCols.isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(sourceRow), Integer.class);
                index++;
            }
            if (!writeTimeStampCols.isEmpty()) {
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

    public BoundStatement bindInsertOneToOne(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        return bindInsertOneToOne(insertStatement, sourceRow, astraRow, null, null);
    }

    public List<BoundStatement> bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        List<BoundStatement> rtnList = new ArrayList<>();

        // If there is no map column to expand, we return a single row as previously
        if (mapToExpandIndex < 0) {
            rtnList.add(bindInsertOneToOne(insertStatement, sourceRow, astraRow));
            return rtnList;
        }

        // pull the map to expand out of the sourceRow
        Map colMap = (Map) getData(selectColTypes.get(mapToExpandIndex), mapToExpandIndex, sourceRow);

        // iterate over map elements, inserting one row for each map entry
        for (Object mapKey : colMap.keySet()) {
            rtnList.add(bindInsertOneToOne(insertStatement, sourceRow, astraRow, mapKey, colMap.get(mapKey)));
        }
        return rtnList;
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
        return selectFromAstra(selectStatement, sourceRow, null);
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow, Object mapKey) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < idColTypes.size(); index++) {
            if (null != mapKey && index == mapToExpandIndex) {
                boundSelectStatement = boundSelectStatement.set(index, mapKey, mapToExpandKeyType);
            } else {
                boundSelectStatement = getBoundStatement(sourceRow, boundSelectStatement, index, idColTypes);
            }
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
                        getKey(sourceRow), allCols[index]);
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(sourceRow), allCols[index]);
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(sourceRow), allCols[index], Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

}
