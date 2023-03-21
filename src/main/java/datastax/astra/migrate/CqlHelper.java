package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

public class CqlHelper {

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public enum CQL {
        ORIGIN_SELECT,
        TARGET_INSERT,
        TARGET_SELECT_BY_PK
    }

    private PropertyHelper propertyHelper;
    private CqlSession originSession;
    private CqlSession targetSession;

    private Map<CQL,String> cqlStatementMap = new HashMap<>();
    private Map<CQL,PreparedStatement> preparedStatementMap = new HashMap<>();

    ////////////////////////////////////////////////
    // These from BaseJobSession
    protected ConsistencyLevel readConsistencyLevel;
    protected ConsistencyLevel writeConsistencyLevel;
    protected PreparedStatement originSelectStatement;
    protected PreparedStatement targetSelectByPKStatement;
    protected PreparedStatement targetInsertStatement;
    private boolean isJobMigrateRowsFromFile;

    private List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();
    private List<MigrateDataType> idColTypes = new ArrayList<MigrateDataType>();
    private List<Integer> updateSelectMapping = new ArrayList<Integer>();

    private Integer batchSize = 1;
    private Integer fetchSizeInRows = 1000;
    private Integer printStatsAfter = 100000;

    private Boolean writeTimeStampFilter = Boolean.FALSE;
    private Long minWriteTimeStampFilter = 0l;
    private Long maxWriteTimeStampFilter = Long.MAX_VALUE;
    private Long customWritetime = 0l;

    private List<Integer> writeTimeStampCols = new ArrayList<Integer>();
    private List<Integer> ttlCols = new ArrayList<Integer>();
    private Boolean isCounterTable;

    private String originKeyspaceTable;
    private String targetKeyspaceTable;

    private Boolean hasRandomPartitioner;
    private Boolean filterData;
    private String filterColName;
    private MigrateDataType filterColType;
    private Integer filterColIndex;
    private String filterColValue;

    private List<String> allCols;
    private String tsReplaceValStr;
    private long tsReplaceVal;
    ////////////////////////////////////////////////

    public CqlHelper() {
        this.propertyHelper = PropertyHelper.getInstance();

        readConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.READ_CL));
        writeConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.WRITE_CL));;
    }

    public void setJobMigrateRowsFromFile(Boolean jobMigrateRowsFromFile) {
        isJobMigrateRowsFromFile = jobMigrateRowsFromFile;
    }

    public void setOriginSession(CqlSession originSession) {
        this.originSession = originSession;
    }

    public void setTargetSession(CqlSession targetSession) {
        this.targetSession = targetSession;
    }

    public CqlSession getOriginSession() {
        return originSession;
    }

    public CqlSession getTargetSession() {
        return targetSession;
    }

    public boolean hasRandomPartitioner() {
        return hasRandomPartitioner;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public Integer getFetchSizeInRows() {
        return fetchSizeInRows;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public boolean hasWriteTimestampFilter() {
        return writeTimeStampFilter;
    }

    public boolean isCounterTable() {
        return isCounterTable;
    }

    public boolean hasFilterColumn() {
        return filterData;
    }

    public MigrateDataType getFilterColType() {
        return filterColType;
    }

    public Integer getFilterColIndex() {
        return filterColIndex;
    }

    public String getFilterColValue() {
        return filterColValue;
    }

    public Long getMinWriteTimeStampFilter() {
        return minWriteTimeStampFilter;
    }

    public Long getMaxWriteTimeStampFilter() {
        return maxWriteTimeStampFilter;
    }

    public List<MigrateDataType> getSelectColTypes() {
        return selectColTypes;
    }

    public List<MigrateDataType> getIdColTypes() {
        return idColTypes;
    }

    public void initialize() {
        originKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
        targetKeyspaceTable = propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);

        batchSize = propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
        fetchSizeInRows = propertyHelper.getInteger(KnownProperties.READ_FETCH_SIZE);

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
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Origin Keyspace Table: {}", originKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: {}", targetKeyspaceTable);
        logger.info("PARAM -- TTLCols: {}", ttlCols);
        logger.info("PARAM -- WriteTimestampFilterCols: {}", writeTimeStampCols);
        logger.info("PARAM -- WriteTimestampFilter: {}", writeTimeStampFilter);
        if (writeTimeStampFilter) {
            logger.info("PARAM -- minWriteTimeStampFilter: {} datetime is {}", minWriteTimeStampFilter,
                    Instant.ofEpochMilli(minWriteTimeStampFilter / 1000));
            logger.info("PARAM -- maxWriteTimeStampFilter: {} datetime is {}", maxWriteTimeStampFilter,
                    Instant.ofEpochMilli(maxWriteTimeStampFilter / 1000));
        }

        filterData = propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_COLUMN_ENABLED);
        filterColName = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_COLUMN_NAME);
        filterColType = propertyHelper.getMigrationType(KnownProperties.ORIGIN_FILTER_COLUMN_TYPE);
        filterColIndex = propertyHelper.getInteger(KnownProperties.ORIGIN_FILTER_COLUMN_INDEX);
        filterColValue = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_COLUMN_VALUE);

        String selectCols = String.join(",", propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES));
        String partitionKey = String.join(",", propertyHelper.getStringList(KnownProperties.ORIGIN_PARTITION_KEY));
        String originSelectCondition = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_CONDITION);
        if (null != originSelectCondition && !originSelectCondition.isEmpty() && !originSelectCondition.trim().toUpperCase().startsWith("AND")) {
            originSelectCondition = " AND " + originSelectCondition;
            propertyHelper.setProperty(KnownProperties.ORIGIN_FILTER_CONDITION, originSelectCondition);
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
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + originKeyspaceTable +
                    " where token(" + partitionKey.trim() + ") >= ? and token(" + partitionKey.trim() + ") <= ?  " +
                    originSelectCondition + " ALLOW FILTERING";
        } else {
            fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + originKeyspaceTable + " where " + insertBinds;
        }

        cqlStatementMap.put(CQL.ORIGIN_SELECT, fullSelectQuery);

        String targetSelectQuery = "select " + insertCols + " from " + targetKeyspaceTable + " where " + insertBinds;
        cqlStatementMap.put(CQL.TARGET_SELECT_BY_PK, targetSelectQuery);

        String targetInsertQuery;
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
        if (isCounterTable) {
            updateSelectMapping = propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEX);
            targetInsertQuery = propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_CQL);
            logger.info("PARAM -- TARGET INSERT Query used: {}", targetInsertQuery);
            cqlStatementMap.put(CQL.TARGET_INSERT, targetInsertQuery);
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
            cqlStatementMap.put(CQL.TARGET_INSERT, fullInsertQuery);
        }
        fullSelectQuery = getCqlString(CQL.ORIGIN_SELECT);
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", fullSelectQuery);
        originSelectStatement = originSession.prepare(fullSelectQuery);
        preparedStatementMap.put(CQL.ORIGIN_SELECT, originSelectStatement);

        targetSelectByPKStatement = targetSession.prepare(getCqlString(CQL.TARGET_SELECT_BY_PK));
        preparedStatementMap.put(CQL.TARGET_SELECT_BY_PK, targetSelectByPKStatement);

        hasRandomPartitioner = propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);

        targetInsertStatement = targetSession.prepare(getCqlString(CQL.TARGET_INSERT));
        preparedStatementMap.put(CQL.TARGET_INSERT, targetInsertStatement);

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (null != propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS))
            tsReplaceVal = propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS);

    }

    public String getCqlString(CQL CQL) {
        return cqlStatementMap.get(CQL);
    }

    public PreparedStatement getPreparedStatement(CQL CQL) {
        return preparedStatementMap.get(CQL);
    }

    private BoundStatement getBoundStatement(Row originRow, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = getData(dataTypeObj, index, originRow);

        // Handle rows with blank values in primary-key fields
        if (index < idColTypes.size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, dataTypeObj.typeClass, originRow);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, dataTypeObj.typeClass);
        return boundSelectStatement;
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

            if (null != ttlCols && !ttlCols.isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(originRow), Integer.class);
                index++;
            }
            if (null != writeTimeStampCols && !writeTimeStampCols.isEmpty()) {
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

    public BoundStatement selectFromTargetByPK(PreparedStatement selectStatement, Row originRow) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < idColTypes.size(); index++) {
            boundSelectStatement = getBoundStatement(originRow, boundSelectStatement, index, idColTypes);
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private int getLargestTTL(Row originRow) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> originRow.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row originRow) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> originRow.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
    }

    private Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row originRow) {
        return handleBlankInPrimaryKey(index, colData, dataType, originRow, true);
    }

    public Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row originRow, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        getKey(originRow), allCols.get(index));
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(originRow), allCols.get(index));
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(originRow), allCols.get(index), Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

    public String getKey(Row originRow) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, originRow));
            } else {
                key.append(" %% " + getData(dataType, index, originRow));
            }
        }

        return key.toString();
    }

    public List<MigrateDataType> getTypes(String types) {
        List<MigrateDataType> dataTypes = new ArrayList<MigrateDataType>();
        for (String type : types.split(",")) {
            dataTypes.add(new MigrateDataType(type));
        }

        return dataTypes;
    }

    public Object getData(MigrateDataType dataType, int index, Row originRow) {
        if (dataType.typeClass == Map.class) {
            return originRow.getMap(index, dataType.subTypes.get(0), dataType.subTypes.get(1));
        } else if (dataType.typeClass == List.class) {
            return originRow.getList(index, dataType.subTypes.get(0));
        } else if (dataType.typeClass == Set.class) {
            return originRow.getSet(index, dataType.subTypes.get(0));
        } else if (isCounterTable && dataType.typeClass == Long.class) {
            Object data = originRow.get(index, dataType.typeClass);
            if (data == null) {
                return new Long(0);
            }
        }

        return originRow.get(index, dataType.typeClass);
    }
}
