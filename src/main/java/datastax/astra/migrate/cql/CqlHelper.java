package datastax.astra.migrate.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.Util;
import datastax.astra.migrate.cql.features.*;
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
        TARGET_SELECT_ORIGIN_BY_PK
    }

    // Values with public Getters
    private final Map<CQL,String> cqlMap = new HashMap<>(CQL.values().length);
    private final Map<CQL,PreparedStatement> preparedStatementMap = new HashMap<>(CQL.values().length);
    private final Map<Featureset, Feature> featureMap = new HashMap<>(Featureset.values().length);

    private CqlSession originSession;
    private CqlSession targetSession;

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    private boolean isJobMigrateRowsFromFile = false;
    private final PropertyHelper propertyHelper;

    ////////////////////////////////////////////////

    public CqlHelper() {
        this.propertyHelper = PropertyHelper.getInstance();
    }

    public boolean initialize() {
        boolean validInit = true;

        readConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.READ_CL));
        writeConsistencyLevel = Util.mapToConsistencyLevel(propertyHelper.getString(KnownProperties.WRITE_CL));;

        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = FeatureFactory.getFeature(f); // FeatureFactory throws an RTE if the feature is not implemented
            if (!feature.initialize(this.propertyHelper))
                validInit = false;
            else
                featureMap.put(f, feature);
        }

        if (hasWriteTimestampFilter()) {
            propertyHelper.setProperty(KnownProperties.SPARK_BATCH_SIZE, 1);
        }

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", getBatchSize());
        logger.info("PARAM -- Read Fetch Size: {}", getFetchSizeInRows());
        logger.info("PARAM -- Origin Keyspace Table: {}", getOriginKeyspaceTable());
        logger.info("PARAM -- Target Keyspace Table: {}", getTargetKeyspaceTable());
        logger.info("PARAM -- TTLCols: {}", getTtlCols());
        logger.info("PARAM -- WriteTimestampCols: {}", getWriteTimeStampCols());
        logger.info("PARAM -- WriteTimestampFilter: {}", hasWriteTimestampFilter());
        if (hasWriteTimestampFilter()) {
            logger.info("PARAM -- minWriteTimeStampFilter: {} datetime is {}", getMinWriteTimeStampFilter(),
                    Instant.ofEpochMilli(getMinWriteTimeStampFilter() / 1000));
            logger.info("PARAM -- maxWriteTimeStampFilter: {} datetime is {}", getMaxWriteTimeStampFilter(),
                    Instant.ofEpochMilli(getMaxWriteTimeStampFilter() / 1000));
        }
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", getCql(CQL.ORIGIN_SELECT));
        logger.info("PARAM -- TARGET INSERT Query used: {}", getCql(CQL.TARGET_INSERT));
        logger.info("PARAM -- TARGET SELECT Query used: {}", getCql(CQL.TARGET_SELECT_ORIGIN_BY_PK));

        return validInit;
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row originRow, Row targetRow) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        int selectColTypesSize = getSelectColTypes().size();
        if (isCounterTable()) {
            for (int index = 0; index < selectColTypesSize; index++) {
                MigrateDataType dataType = getSelectColTypes().get(getOriginColumnIndexes().get(index));
                // compute the counter delta if reading from target for the difference
                if (targetRow != null && index < (selectColTypesSize - getIdColTypes().size())) {
                    boundInsertStatement = boundInsertStatement.set(index, (originRow.getLong(getOriginColumnIndexes().get(index)) - targetRow.getLong(getOriginColumnIndexes().get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, getOriginColumnIndexes().get(index), originRow), dataType.getType());
                }
            }
        } else {
            int index = 0;
            for (index = 0; index < selectColTypesSize; index++) {
                boundInsertStatement = getBoundStatement(originRow, boundInsertStatement, index, getSelectColTypes());
                if (boundInsertStatement == null) return null;
            }

            if (null != getTtlCols() && !getTtlCols().isEmpty()) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(originRow), Integer.class);
                index++;
            }
            if (null != getWriteTimeStampCols() && !getWriteTimeStampCols().isEmpty()) {
                if (getCustomWriteTime() > 0) {
                    boundInsertStatement = boundInsertStatement.set(index, getCustomWriteTime(), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getLargestWriteTimeStamp(originRow), Long.class);
                }
            }
        }

        // Batch insert for large records may take longer, hence 10 secs to avoid timeout errors
        return boundInsertStatement.setTimeout(Duration.ofSeconds(10));
    }

    public BoundStatement selectFromTargetByPK(PreparedStatement selectStatement, Row row) {
        BoundStatement boundSelectStatement = selectStatement.bind().setConsistencyLevel(readConsistencyLevel);
        for (int index = 0; index < getIdColTypes().size(); index++) {
            boundSelectStatement = getBoundStatement(row, boundSelectStatement, index, getIdColTypes());
            if (boundSelectStatement == null) return null;
        }

        return boundSelectStatement;
    }

    private String cqlOriginSelect() {
        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        if (null != getTtlCols()) {
            getTtlCols().forEach(col -> {
                selectTTLWriteTimeCols.append(",TTL(" + getOriginColumnNames().get(col) + ")");
            });
        }
        if (null != getWriteTimeStampCols()) {
            getWriteTimeStampCols().forEach(col -> {
                selectTTLWriteTimeCols.append(",WRITETIME(" + getOriginColumnNames().get(col) + ")");
            });
        }

        String fullSelectQuery;
        if (!isJobMigrateRowsFromFile) {
            String partitionKey = propertyHelper.getAsString(KnownProperties.ORIGIN_PARTITION_KEY).trim();
            fullSelectQuery = "SELECT " + propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_NAMES) + selectTTLWriteTimeCols + " FROM " + getOriginKeyspaceTable() +
                    " WHERE TOKEN(" + partitionKey + ") >= ? AND TOKEN(" + partitionKey + ") <= ? " +
                    getFeature(Featureset.ORIGIN_FILTER).getAsString(OriginFilterCondition.Property.CONDITION) +
                    " ALLOW FILTERING";
        } else {
            String keyBinds = "";
            for (String key : propertyHelper.getStringList(KnownProperties.TARGET_PRIMARY_KEY)) {
                if (keyBinds.isEmpty()) {
                    keyBinds = key + "=?";
                } else {
                    keyBinds += " AND " + key + "=?";
                }
            }
            fullSelectQuery = "SELECT " + propertyHelper.getAsString(KnownProperties.ORIGIN_COLUMN_NAMES) + selectTTLWriteTimeCols + " FROM " + getOriginKeyspaceTable() + " WHERE " + keyBinds;
        }
        return fullSelectQuery;
    }

    private String cqlTargetInsert() {
        String targetInsertQuery = null;
        if (isCounterTable()) {
            targetInsertQuery = propertyHelper.getString(KnownProperties.ORIGIN_COUNTER_CQL);
        } else {
            String insertBinds = "";
            for (String key : propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES)) {
                if (insertBinds.isEmpty()) {
                    insertBinds = "?";
                } else {
                    insertBinds += ",?";
                }
            }
            targetInsertQuery = "INSERT INTO " + getTargetKeyspaceTable() + " (" + propertyHelper.getAsString(KnownProperties.TARGET_COLUMN_NAMES) + ") VALUES (" + insertBinds + ")";
            if (null != getTtlCols() && !getTtlCols().isEmpty()) {
                targetInsertQuery += " USING TTL ?";
                if (null != getWriteTimeStampCols() &&  !getWriteTimeStampCols().isEmpty()) {
                    targetInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (null != getWriteTimeStampCols() &&  !getWriteTimeStampCols().isEmpty()) {
                targetInsertQuery += " USING TIMESTAMP ?";
            }
        }
        return targetInsertQuery;
    }

    private String cqlTargetSelectOriginByPK() {
        String keyBinds = "";
        for (String key : propertyHelper.getStringList(KnownProperties.TARGET_PRIMARY_KEY)) {
            if (keyBinds.isEmpty()) {
                keyBinds = key + "=?";
            } else {
                keyBinds += " AND " + key + "=?";
            }
        }

        return "SELECT " + propertyHelper.getAsString(KnownProperties.TARGET_COLUMN_NAMES) + " FROM " + getTargetKeyspaceTable() + " WHERE " + keyBinds;
    }

    public long getLargestWriteTimeStamp(Row row) {
        return IntStream.range(0, getWriteTimeStampCols().size())
                .mapToLong(i -> row.getLong(getSelectColTypes().size() + getTtlCols().size() + i)).max().getAsLong();
    }

    public Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row row, boolean logWarn) {
        // Handle rows with blank values for 'String' data-type in primary-key fields
        if (index < getIdColTypes().size() && colData == null && dataType == String.class) {
            if (logWarn) {
                logger.warn("For row with Key: {}, found String primary-key column {} with blank value",
                        getKey(row), getOriginColumnNames().get(index));
            }
            return Optional.of("");
        }

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (index < getIdColTypes().size() && colData == null && dataType == Instant.class) {
            Long tsReplaceVal = getReplaceMissingTs();
            if (null == tsReplaceVal) {
                logger.error("Skipping row with Key: {} as Timestamp primary-key column {} has invalid blank value. " +
                        "Alternatively rerun the job with --conf spark.target.replace.blankTimestampKeyUsingEpoch=\"<fixed-epoch-value>\" " +
                        "option to replace the blanks with a fixed timestamp value", getKey(row), getOriginColumnNames().get(index));
                return Optional.empty();
            }
            if (logWarn) {
                logger.warn("For row with Key: {}, found Timestamp primary-key column {} with invalid blank value. " +
                        "Using value {} instead", getKey(row), getOriginColumnNames().get(index), Instant.ofEpochSecond(tsReplaceVal));
            }
            return Optional.of(Instant.ofEpochSecond(tsReplaceVal));
        }

        return Optional.of(colData);
    }

    public String getKey(Row row) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < getIdColTypes().size(); index++) {
            MigrateDataType dataType = getIdColTypes().get(index);
            if (index == 0) {
                key.append(getData(dataType, index, row));
            } else {
                key.append(" %% " + getData(dataType, index, row));
            }
        }

        return key.toString();
    }

    public Object getData(MigrateDataType dataType, int index, Row row) {
        if (dataType.getType() == Map.class) {
            return row.getMap(index, dataType.getSubTypes().get(0), dataType.getSubTypes().get(1));
        } else if (dataType.getType() == List.class) {
            return row.getList(index, dataType.getSubTypes().get(0));
        } else if (dataType.getType() == Set.class) {
            return row.getSet(index, dataType.getSubTypes().get(0));
        } else if (isCounterTable() && dataType.getType() == Long.class) {
            Object data = row.get(index, dataType.getType());
            if (data == null) {
                return new Long(0);
            }
        }

        return row.get(index, dataType.getType());
    }

    private BoundStatement getBoundStatement(Row originRow, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = getData(dataTypeObj, index, originRow);

        // Handle rows with blank values in primary-key fields
        if (index < getIdColTypes().size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, dataTypeObj.getType(), originRow);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, dataTypeObj.getType());
        return boundSelectStatement;
    }

    private int getLargestTTL(Row row) {
        return IntStream.range(0, getTtlCols().size())
                .map(i -> row.getInt(getSelectColTypes().size() + i)).max().getAsInt();
    }

    private Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row row) {
        return handleBlankInPrimaryKey(index, colData, dataType, row, true);
    }

    // Setters
    public void setJobMigrateRowsFromFile(Boolean jobMigrateRowsFromFile) {
        isJobMigrateRowsFromFile = jobMigrateRowsFromFile;
    }

    public void setOriginSession(CqlSession originSession) {
        this.originSession = originSession;
    }

    public void setTargetSession(CqlSession targetSession) {
        this.targetSession = targetSession;
    }

    // Getters
    public String getCql(CQL cql) {
        if (!cqlMap.containsKey(cql)) {
            switch (cql) {
                case ORIGIN_SELECT:
                    cqlMap.put(cql, cqlOriginSelect());
                    break;
                case TARGET_INSERT:
                    cqlMap.put(cql, cqlTargetInsert());
                    break;
                case TARGET_SELECT_ORIGIN_BY_PK:
                    cqlMap.put(cql, cqlTargetSelectOriginByPK());
                    break;
            }
        }
        return cqlMap.get(cql);
    }

    public PreparedStatement getPreparedStatement(CQL cql) {
        abendIfSessionsNotSet();

        if (!preparedStatementMap.containsKey(cql)) {
            switch (cql) {
                case ORIGIN_SELECT:
                    preparedStatementMap.put(cql, getOriginSession().prepare(getCql(cql)));
                    break;
                case TARGET_INSERT:
                case TARGET_SELECT_ORIGIN_BY_PK:
                    preparedStatementMap.put(cql, getTargetSession().prepare(getCql(cql)));
                    break;
            }
        }
        return preparedStatementMap.get(cql);
    }

    public CqlSession getOriginSession() {
        abendIfSessionsNotSet();
        return originSession;
    }

    public CqlSession getTargetSession() {
        abendIfSessionsNotSet();
        return targetSession;
    }

    public boolean hasRandomPartitioner() {
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public Integer getFetchSizeInRows() {
        return propertyHelper.getInteger(KnownProperties.READ_FETCH_SIZE);
    }

    public Integer getBatchSize() {
        return propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
    }

    public boolean hasWriteTimestampFilter() {
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_WRITETS_ENABLED);
    }

    public boolean isCounterTable() {
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);
    }

    public boolean hasFilterColumn() {
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_COLUMN_ENABLED);
    }

    public MigrateDataType getFilterColType() {
        return propertyHelper.getMigrationType(KnownProperties.ORIGIN_FILTER_COLUMN_TYPE);
    }

    public Integer getFilterColIndex() {
        return propertyHelper.getInteger(KnownProperties.ORIGIN_FILTER_COLUMN_INDEX);
    }

    public String getFilterColValue() {
        return propertyHelper.getString(KnownProperties.ORIGIN_FILTER_COLUMN_VALUE);
    }

    public Long getMinWriteTimeStampFilter() {
        return propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MIN);
    }

    public Long getMaxWriteTimeStampFilter() {
        return propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MAX);
    }

    public List<MigrateDataType> getSelectColTypes() {
        return propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES);
    }

    public List<MigrateDataType> getIdColTypes() {
        return propertyHelper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES);
    }

    // These getters have no usage outside this class
    private List<Integer> getTtlCols() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_COLS);
    }

    private List<Integer> getWriteTimeStampCols() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_COLS);
    }

    private Long getCustomWriteTime() {
        return propertyHelper.getLong(KnownProperties.TARGET_CUSTOM_WRITETIME);
    }

    private List<String> getOriginColumnNames() {
        return propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);
    }

    private List<Integer> getOriginColumnIndexes() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEXES);
    }

    private Long getReplaceMissingTs() {
        return propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS);
    }

    private String getOriginKeyspaceTable() {
        return propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
    }

    private String getTargetKeyspaceTable() {
        return propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);
    }

    private void abendIfSessionsNotSet() {
        if (null == originSession || originSession.isClosed() || null == targetSession || targetSession.isClosed()) {
            throw new RuntimeException("Origin and/or Target sessions are either not set, or are closed");
        }
    }

    public Feature getFeature(Featureset featureEnum) {
        return featureMap.get(featureEnum);
    }
}
