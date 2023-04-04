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

import datastax.astra.migrate.cql.statements.*;

public class CqlHelper {

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    // Values with public Getters
    private final Map<Featureset, Feature> featureMap = new HashMap<>(Featureset.values().length);

    private CqlSession originSession;
    private CqlSession targetSession;

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    private PKFactory pkFactory;
    private OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement;
    private OriginSelectByPKStatement originSelectByPKStatement;
    private TargetInsertStatement targetInsertStatement;
    private TargetUpdateStatement targetUpdateStatement;
    private TargetSelectByPKStatement targetSelectByPKStatement;

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

        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = getFeature(f);
            if (isFeatureEnabled(f))
                feature.alterProperties(this.propertyHelper);
        }

        if (hasWriteTimestampFilter()) {
            propertyHelper.setProperty(KnownProperties.SPARK_BATCH_SIZE, 1);
        }

        pkFactory = new PKFactory(propertyHelper, this);
        originSelectByPartitionRangeStatement = new OriginSelectByPartitionRangeStatement(propertyHelper,this);
        originSelectByPKStatement = new OriginSelectByPKStatement(propertyHelper,this);
        targetInsertStatement = new TargetInsertStatement(propertyHelper,this);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper,this);
        targetSelectByPKStatement = new TargetSelectByPKStatement(propertyHelper,this);

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
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", originSelectByPartitionRangeStatement.getCQL());
        logger.info("PARAM -- TARGET INSERT Query used: {}", targetInsertStatement.getCQL());
        logger.info("PARAM -- TARGET UPDATE Query used: {}", targetUpdateStatement.getCQL());
        logger.info("PARAM -- TARGET SELECT Query used: {}", targetSelectByPKStatement.getCQL());

        return validInit;
    }

    public PKFactory getPKFactory() {return pkFactory;}
    public OriginSelectByPartitionRangeStatement getOriginSelectByPartitionRangeStatement() {return originSelectByPartitionRangeStatement;}
    public OriginSelectByPKStatement getOriginSelectByPKStatement() {return originSelectByPKStatement;}
    public TargetInsertStatement getTargetInsertStatement() {
        return targetInsertStatement;
    }
    public TargetUpdateStatement getTargetUpdateStatement() {return targetUpdateStatement;}
    public TargetSelectByPKStatement getTargetSelectByPKStatement() {
        return targetSelectByPKStatement;
    }

    public List<BoundStatement> bindInsert(PreparedStatement insertStatement, Row originRow, Row targetRow) {
        List<BoundStatement> rtnList = new ArrayList<>();
        if (isFeatureEnabled(Featureset.EXPLODE_MAP)) {
            Feature explodeMapFeature = getFeature(Featureset.EXPLODE_MAP);
            Map maptoExplode = (Map) getData(explodeMapFeature.getMigrateDataType(ExplodeMap.Property.MAP_COLUMN_TYPE),
                    explodeMapFeature.getInteger(ExplodeMap.Property.MAP_COLUMN_INDEX),
                    originRow);
            for (Object key : maptoExplode.keySet()) {
                rtnList.add(bindInsertOneRow(insertStatement, originRow, targetRow, key, maptoExplode.get(key)));
            }
        } else {
            rtnList.add(bindInsertOneRow(insertStatement, originRow, targetRow, null, null));
        }
        return rtnList;
    }

    public BoundStatement bindInsertOneRow(PreparedStatement insertStatement, Row originRow, Row targetRow) {
        return bindInsertOneRow(insertStatement, originRow, targetRow, null, null);
    }

    public BoundStatement bindInsertOneRow(PreparedStatement insertStatement, Row originRow, Row targetRow, Object mapKey, Object mapValue) {
        BoundStatement boundInsertStatement = insertStatement.bind().setConsistencyLevel(writeConsistencyLevel);

        int originColTypesSize = getOriginColTypes().size();
        if (isCounterTable()) {
            for (int index = 0; index < originColTypesSize; index++) {
                MigrateDataType dataType = getOriginColTypes().get(getOriginColumnIndexes().get(index));
                // compute the counter delta if reading from target for the difference
                if (targetRow != null && index < (originColTypesSize - getIdColTypes().size())) {
                    boundInsertStatement = boundInsertStatement.set(index, (originRow.getLong(getOriginColumnIndexes().get(index)) - targetRow.getLong(getOriginColumnIndexes().get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, getOriginColumnIndexes().get(index), originRow), dataType.getTypeClass());
                }
            }
        } else {
            int index = 0;
            // This loops over the selected columns and binds each type to the boundInsertStatement
            Feature explodeMapFeature = getFeature(Featureset.EXPLODE_MAP);
            for (index = 0; index < originColTypesSize; index++) {
                if (mapKey != null &&
                        isFeatureEnabled(Featureset.EXPLODE_MAP) &&
                        index == explodeMapFeature.getInteger(ExplodeMap.Property.MAP_COLUMN_INDEX)) {
                    // This substitutes the map column with the key and value types of the map
                    boundInsertStatement = boundInsertStatement.set(index, mapKey, explodeMapFeature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE).getTypeClass());
                    // Add an 'extra' column to the statement, which will also increase the loop limit
                    index++;
                    originColTypesSize++;
                    // And then bind the map value to the next column
                    boundInsertStatement = boundInsertStatement.set(index, mapValue, explodeMapFeature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE).getTypeClass());

                }
                else {
                    // This is the previous behaviour, and when on any column that is not the map to explode
                    boundInsertStatement = getBoundStatement(originRow, boundInsertStatement, index, getOriginColTypes());
                }
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




    public long getLargestWriteTimeStamp(Row row) {
        return IntStream.range(0, getWriteTimeStampCols().size())
                .mapToLong(i -> row.getLong(getOriginColTypes().size() + getTtlCols().size() + i)).max().getAsLong();
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
        if (dataType.getTypeClass() == Map.class) {
            return row.getMap(index, dataType.getSubTypeClasses().get(0), dataType.getSubTypeClasses().get(1));
        } else if (dataType.getTypeClass() == List.class) {
            return row.getList(index, dataType.getSubTypeClasses().get(0));
        } else if (dataType.getTypeClass() == Set.class) {
            return row.getSet(index, dataType.getSubTypeClasses().get(0));
        } else if (isCounterTable() && dataType.getTypeClass() == Long.class) {
            Object data = row.get(index, dataType.getTypeClass());
            if (data == null) {
                return new Long(0);
            }
        }

        return row.get(index, dataType.getTypeClass());
    }

    private BoundStatement getBoundStatement(Row originRow, BoundStatement boundSelectStatement, int index,
                                             List<MigrateDataType> cols) {
        MigrateDataType dataTypeObj = cols.get(index);
        Object colData = getData(dataTypeObj, index, originRow);

        // Handle rows with blank values in primary-key fields
        if (index < getIdColTypes().size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(index, colData, dataTypeObj.getTypeClass(), originRow);
            if (!optionalVal.isPresent()) {
                return null;
            }
            colData = optionalVal.get();
        }
        boundSelectStatement = boundSelectStatement.set(index, colData, dataTypeObj.getTypeClass());
        return boundSelectStatement;
    }

    private int getLargestTTL(Row row) {
        return IntStream.range(0, getTtlCols().size())
                .map(i -> row.getInt(getOriginColTypes().size() + i)).max().getAsInt();
    }

    private Optional<Object> handleBlankInPrimaryKey(int index, Object colData, Class dataType, Row row) {
        return handleBlankInPrimaryKey(index, colData, dataType, row, true);
    }

    // Setters
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
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }
    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
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

    public List<MigrateDataType> getOriginColTypes() {
        return propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES);
    }

    public List<MigrateDataType> getIdColTypes() {
        return propertyHelper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES);
    }

    // These getters have no usage outside this class
    public List<Integer> getTtlCols() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_COLS);
    }

    public List<Integer> getWriteTimeStampCols() {
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

    public Feature getFeature(Featureset featureEnum) {
        return featureMap.get(featureEnum);
    }

    public Boolean isFeatureEnabled(Featureset featureEnum) {
        if (!featureMap.containsKey(featureEnum)) {
            return false;
        }
        return featureMap.get(featureEnum).isEnabled();
    }
}
