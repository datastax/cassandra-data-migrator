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

    public enum CqlStatementType {
        ORIGIN_SELECT,
        TARGET_INSERT,
        TARGET_SELECT_BY_PK
    }

    private PropertyHelper propertyHelper;
    private CqlSession originSession;
    private CqlSession targetSession;
    private boolean isJobMigrateRowsFromFile;
    private BaseJobSession baseJobSession;

    private Map<CqlStatementType,String> cqlStatementTypeStringMap = new HashMap<>();

    ////////////////////////////////////////////////
    // These from BaseJobSession
    protected ConsistencyLevel readConsistencyLevel;
    protected ConsistencyLevel writeConsistencyLevel;

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

    private String sourceKeyspaceTable;
    private String targetKeyspaceTable;

    private Boolean hasRandomPartitioner;
    private Boolean filterData;
    private String filterColName;
    private String filterColType;
    private Integer filterColIndex;
    private String filterColValue;

    private List<String> allCols;
    private String tsReplaceValStr;
    private long tsReplaceVal;
    ////////////////////////////////////////////////

    public CqlHelper(PropertyHelper propertyHelper, CqlSession originSession, CqlSession targetSession, boolean isJobMigrateRowsFromFile, BaseJobSession baseJobSession) {
        this.propertyHelper = propertyHelper;
        this.originSession = originSession;
        this.targetSession = targetSession;
        this.isJobMigrateRowsFromFile = isJobMigrateRowsFromFile;
        this.baseJobSession = baseJobSession;

        readConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.READ_CL));
        writeConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.WRITE_CL));;
    }

    public void initialize() {
        sourceKeyspaceTable = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
        targetKeyspaceTable = propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);

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

        cqlStatementTypeStringMap.put(CqlStatementType.ORIGIN_SELECT, fullSelectQuery);

        String astraSelectQuery = "select " + insertCols + " from " + targetKeyspaceTable + " where " + insertBinds;
        cqlStatementTypeStringMap.put(CqlStatementType.TARGET_SELECT_BY_PK, astraSelectQuery);

        String targetInsertQuery;
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
        if (isCounterTable) {
            updateSelectMapping = propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEX);
            targetInsertQuery = propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_CQL);
            logger.info("PARAM -- TARGET INSERT Query used: {}", targetInsertQuery);
            cqlStatementTypeStringMap.put(CqlStatementType.TARGET_INSERT, targetInsertQuery);
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
            cqlStatementTypeStringMap.put(CqlStatementType.TARGET_INSERT, fullInsertQuery);
        }
    }

    public String getCqlString(CqlStatementType cqlStatement) {
        String cql = null;
        switch(cqlStatement) {
            case ORIGIN_SELECT:
                return cqlStatementTypeStringMap.get(CqlStatementType.ORIGIN_SELECT);
            case TARGET_INSERT:
                return cqlStatementTypeStringMap.get(CqlStatementType.TARGET_INSERT);
            case TARGET_SELECT_BY_PK:
                return cqlStatementTypeStringMap.get(CqlStatementType.TARGET_SELECT_BY_PK);
        }
        return cql;
    }

    private BoundStatement getBoundStatement(Row sourceRow, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = baseJobSession.getData(dataTypeObj, index, sourceRow);

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

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        if (isCounterTable) {
            for (int index = 0; index < selectColTypes.size(); index++) {
                MigrateDataType dataType = selectColTypes.get(updateSelectMapping.get(index));
                // compute the counter delta if reading from astra for the difference
                if (astraRow != null && index < (selectColTypes.size() - idColTypes.size())) {
                    boundInsertStatement = boundInsertStatement.set(index, (sourceRow.getLong(updateSelectMapping.get(index)) - astraRow.getLong(updateSelectMapping.get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, baseJobSession.getData(dataType, updateSelectMapping.get(index), sourceRow), dataType.typeClass);
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

    public BoundStatement selectFromTargetByPK(PreparedStatement selectStatement, Row sourceRow) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < idColTypes.size(); index++) {
            boundSelectStatement = getBoundStatement(sourceRow, boundSelectStatement, index, idColTypes);
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private int getLargestTTL(Row sourceRow) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> sourceRow.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> sourceRow.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
    }

    private Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow) {
        return handleBlankInPrimaryKey(index, colData, dataType, sourceRow, true);
    }

    public Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row sourceRow, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        baseJobSession.getKey(sourceRow), allCols.get(index));
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < idColTypes.size() && colData == null && dataType == Instant.class) {
            if (tsReplaceValStr.isEmpty()) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", baseJobSession.getKey(sourceRow), allCols.get(index));
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", baseJobSession.getKey(sourceRow), allCols.get(index), Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }
}
