/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.schema;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.data.DataUtility;
import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.WritetimeTTL;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;

public class CqlTable extends BaseTable {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();
    boolean logTrace = logger.isTraceEnabled();

    private final CqlSession cqlSession;
    private boolean hasRandomPartitioner;
    private final List<String> partitionKeyNames;
    private final List<String> pkNames;
    private final List<Class> pkClasses;
    private final List<Integer> pkIndexes;
    private boolean isCounterTable;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    private List<ColumnMetadata> cqlPartitionKey;
    private List<ColumnMetadata> cqlPrimaryKey;
    private List<ColumnMetadata> cqlAllColumns;
    private Map<String, DataType> columnNameToCqlTypeMap;
    private final List<Class> bindClasses;
    private List<String> writetimeTTLColumns;

    private CqlTable otherCqlTable;
    private List<Integer> correspondingIndexes;
    private final List<Integer> counterIndexes;
    protected Map<Featureset, Feature> featureMap;

    // These defaults address the problem where we cannot insert null values into a PK column
    private final Long defaultForMissingTimestamp;
    private final String defaultForMissingString;

    public CqlTable(IPropertyHelper propertyHelper, boolean isOrigin, CqlSession session) {
        super(propertyHelper, isOrigin);
        this.keyspaceName = unFormatName(keyspaceName);
        this.tableName = unFormatName(tableName);

        this.cqlSession = session;

        // setCqlMetadata(session) will set:
        // - this.cqlPartitionKey : List<ColumnMetadata> of the partition key column(s)
        // - this.cqlPrimaryKey : List<ColumnMetadata> of the primary key (partition key + clustering columns)
        // - this.cqlColumns : List<ColumnMetadata> of all columns on the table
        // - columnNameToCqlTypeMap : Map<String,DataType> of the column name to driver DataType
        setCqlMetadata(session);

        if (null == this.columnNames || this.columnNames.isEmpty()) {
            if (null == this.cqlAllColumns || this.cqlAllColumns.isEmpty()) {
                throw new IllegalArgumentException(
                        "No columns defined for table " + this.keyspaceName + "." + this.tableName);
            }
            this.columnNames = this.cqlAllColumns.stream().map(columnMetadata -> columnMetadata.getName().asInternal())
                    .collect(Collectors.toList());
        }
        this.columnCqlTypes = columnNames.stream().map(columnName -> this.columnNameToCqlTypeMap.get(columnName))
                .collect(Collectors.toList());
        this.bindClasses = columnCqlTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList());

        this.partitionKeyNames = cqlPartitionKey.stream().map(columnMetadata -> columnMetadata.getName().asInternal())
                .collect(Collectors.toList());
        this.pkNames = cqlPrimaryKey.stream().map(columnMetadata -> columnMetadata.getName().asInternal())
                .collect(Collectors.toList());
        List<DataType> pkTypes = cqlPrimaryKey.stream().map(ColumnMetadata::getType).collect(Collectors.toList());
        this.pkClasses = pkTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList());
        this.pkIndexes = pkNames.stream().map(columnNames::indexOf).collect(Collectors.toList());

        this.counterIndexes = IntStream.range(0, columnCqlTypes.size())
                .filter(i -> columnCqlTypes.get(i).equals(DataTypes.COUNTER)).boxed().collect(Collectors.toList());
        this.isCounterTable = !this.counterIndexes.isEmpty();

        this.readConsistencyLevel = mapToConsistencyLevel(propertyHelper.getString(KnownProperties.READ_CL));
        this.writeConsistencyLevel = mapToConsistencyLevel(propertyHelper.getString(KnownProperties.WRITE_CL));

        this.featureMap = new HashMap<>();

        this.defaultForMissingTimestamp = propertyHelper.getLong(KnownProperties.TRANSFORM_REPLACE_MISSING_TS);
        this.defaultForMissingString = "";
    }

    @Override
    public String getKeyspaceTable() {
        return formatName(this.keyspaceName) + "." + formatName(this.tableName);
    }

    public void setFeatureMap(Map<Featureset, Feature> featureMap) {
        this.featureMap = featureMap;
    }

    public Feature getFeature(Featureset featureEnum) {
        return featureMap.get(featureEnum);
    }

    public void setOtherCqlTable(CqlTable otherCqlTable) {
        this.otherCqlTable = otherCqlTable;
        this.correspondingIndexes = calcCorrespondingIndex();
        this.cqlConversions = CqlConversion.getConversions(this, otherCqlTable);
    }

    public CqlTable getOtherCqlTable() {
        return otherCqlTable;
    }

    public boolean isCounterTable() {
        return isCounterTable;
    }

    public List<Integer> getCounterIndexes() {
        return counterIndexes;
    }

    public Class getBindClass(int index) {
        return bindClasses.get(index);
    }

    public int indexOf(String columnName) {
        return columnNames.indexOf(columnName);
    }

    public DataType getDataType(String columnName) {
        return columnNameToCqlTypeMap.get(columnName);
    }

    public DataType getDataType(int index) {
        return ((index < 0 || index >= columnCqlTypes.size()) ? null : columnCqlTypes.get(index));
    }

    public MutableCodecRegistry getCodecRegistry() {
        return (MutableCodecRegistry) cqlSession.getContext().getCodecRegistry();
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public boolean hasRandomPartitioner() {
        return hasRandomPartitioner;
    }

    public Integer getFetchSizeInRows() {
        return propertyHelper.getInteger(KnownProperties.PERF_FETCH_SIZE);
    }

    public Integer getBatchSize() {
        Integer prop = propertyHelper.getInteger(KnownProperties.PERF_BATCH_SIZE);
        WritetimeTTL f = (WritetimeTTL) getFeature(Featureset.WRITETIME_TTL);
        if (isCounterTable || (null != f && f.hasWriteTimestampFilter()) || null == prop || prop < 1)
            return 1;
        else
            return prop;
    }

    private boolean removeMapWithNoValues = propertyHelper
            .getBoolean(KnownProperties.TRANSFORM_MAP_REMOVE_KEY_WITH_NO_VALUE);

    // Adds to the current column list based on the name and type of columns already existing in the table
    // This is useful where a feature is adding a column by name of an existing column.
    // If the column is already present, the bind class is added to the return list.
    public List<Class> extendColumns(List<String> columnNames) {
        List<DataType> columnTypes = columnNames.stream().map(columnName -> this.columnNameToCqlTypeMap.get(columnName))
                .collect(Collectors.toList());
        return extendColumns(columnNames, columnTypes);
    }

    // Adds to the current column list based on the name and type of columns, where the column may or may not
    // be on the table. This is useful for adding virtual/computed columns on a SELECT statement.
    // If the column is already present, the bind class is added to the return list.
    public List<Class> extendColumns(List<String> columnNames, List<DataType> columnTypes) {
        List<Class> rtn = new ArrayList();

        if (null == columnNames || null == columnTypes || columnNames.isEmpty()
                || columnNames.size() != columnTypes.size())
            throw new IllegalArgumentException(
                    "Column name and type must be non-null and non-empty, and must be of the same length");

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            DataType columnType = columnTypes.get(i);

            if (this.columnNames.contains(columnName)) {
                rtn.add(this.bindClasses.get(this.columnNames.indexOf(columnName)));
                continue;
            }

            if (null == columnName || columnName.isEmpty() || null == columnType) {
                logger.warn("Column name and/or type are null or empty for table " + this.keyspaceName + "."
                        + this.tableName + ". Skipping column.");
                rtn.add(null);
                continue;
            }

            this.columnNames.add(columnName);
            this.columnCqlTypes.add(columnType);
            Class bindClass = CqlData.getBindClass(columnType);
            this.bindClasses.add(bindClass);
            rtn.add(bindClass);
            if (DataTypes.COUNTER.equals(columnType)) {
                this.counterIndexes.add(this.columnNames.size() - 1);
                this.isCounterTable = true;
            }
        }

        this.correspondingIndexes = calcCorrespondingIndex();
        this.cqlConversions = CqlConversion.getConversions(this, otherCqlTable);

        return rtn;
    }

    @Override
    public List<String> getColumnNames(boolean format) {
        if (format)
            return formatNames(this.columnNames);
        else
            return this.columnNames;
    }

    public List<String> getPKNames(boolean format) {
        if (format)
            return formatNames(this.pkNames);
        else
            return this.pkNames;
    }

    public List<Class> getPKClasses() {
        return this.pkClasses;
    }

    public static List<String> formatNames(List<String> list) {
        if (null == list || list.isEmpty())
            return list;
        return list.stream().map(CqlTable::formatName).collect(Collectors.toList());
    }

    public static String formatName(String name) {
        if (null == name || name.isEmpty())
            return name;
        if (name.toUpperCase().matches("^[A-Z0-9_]*\\(.*\\)$"))
            return name; // function
        if (name.matches("^\"[^\\s]*\"$"))
            return name; // already quoted
        return CqlIdentifier.fromInternal(name).asCql(true);
    }

    public static List<String> unFormatNames(List<String> list) {
        if (null == list || list.isEmpty())
            return list;
        return list.stream().map(CqlTable::unFormatName).collect(Collectors.toList());
    }

    public static String unFormatName(String name) {
        if (null == name || name.isEmpty())
            return name;
        if (name.matches("^[^\\s\"]+$"))
            return name; // not quoted, assume unformatted
        return CqlIdentifier.fromCql(name).asInternal();
    }

    public List<String> getPartitionKeyNames(boolean format) {
        if (format)
            return formatNames(this.partitionKeyNames);
        else
            return this.partitionKeyNames;
    }

    public Object getData(int index, Row row) {
        return row.get(index, this.getBindClass(index));
    }

    public int byteCount(int index, Object object) {
        if (null == object)
            return 0;
        try {
            return getCodecRegistry().codecFor(getDataType(index)).encode(object, CqlConversion.PROTOCOL_VERSION)
                    .remaining();
        } catch (IllegalArgumentException | CodecNotFoundException | NullPointerException e) {
            throw new IllegalArgumentException(
                    "Unable to encode object " + object + " of Class/DataType " + object.getClass().getName() + "/"
                            + getDataType(index) + " for column " + this.columnNames.get(index),
                    e);
        }
    }

    public Object getAndConvertData(int index, Row row) {
        Object thisObject = getData(index, row);
        if (null == thisObject) {
            return convertNull(index);
        }

        if (removeMapWithNoValues && thisObject instanceof Map) {
            return removeNullValuesFromMap(thisObject);
        }

        if (null != this.cqlConversions) {
            CqlConversion cqlConversion = this.cqlConversions.get(index);
            if (null == cqlConversion) {
                if (logTrace)
                    logger.trace("{} Index:{} not converting:{}", isOrigin ? "origin" : "target", index, thisObject);
                return thisObject;
            } else {
                if (logTrace)
                    logger.trace("{} Index:{} converting:{} via CqlConversion:{}", isOrigin ? "origin" : "target",
                            index, thisObject, cqlConversion);
                return cqlConversion.convert(thisObject);
            }
        } else {
            return thisObject;
        }
    }

    private Object removeNullValuesFromMap(Object thisObject) {
        Set<Map.Entry> ms = (((Map) thisObject).entrySet());
        return ms.stream().filter(e -> (e.getValue() != null))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Object convertNull(int thisIndex) {
        // We do not need to convert nulls for non-PK columns
        int otherIndex = this.getCorrespondingIndex(thisIndex);
        if (!getOtherCqlTable().pkIndexes.contains(otherIndex))
            return null;

        Class c = getOtherCqlTable().bindClasses.get(otherIndex);
        if (Objects.equals(c, String.class)) {
            return defaultForMissingString;
        } else if (Objects.equals(c, Instant.class)) {
            if (null != defaultForMissingTimestamp) {
                return Instant.ofEpochMilli(defaultForMissingTimestamp);
            } else {
                logger.error(
                        "This index {} corresponds to That index {}, which is a primary key column and cannot be null. Consider setting {}.",
                        thisIndex, otherIndex, KnownProperties.TRANSFORM_REPLACE_MISSING_TS);
                return null;
            }
        }

        logger.error("This index {} corresponds to That index {}, which is a primary key column and cannot be null.",
                thisIndex, otherIndex);
        return null;
    }

    public Integer getCorrespondingIndex(int index) {
        if (index < 0)
            return index;
        return this.correspondingIndexes.get(index);
    }

    private List<Integer> calcCorrespondingIndex() {
        List<Integer> rtn = new ArrayList<>();
        List<String> thisColumnNames = this.getColumnNames(false);
        List<String> thatColumnNames = this.otherCqlTable.getColumnNames(false);
        Map<String, String> thisToThatNameMap = DataUtility.getThisToThatColumnNameMap(propertyHelper, this,
                otherCqlTable);

        for (String thisColumnName : thisColumnNames) {
            // Iterate over the thisColumnNames. If there is an entry on the thisToThatNameMap
            // then there a corresponding column name, and we place the column index on the list.
            // Otherwise, we place -1, indicating this column name is not present in the other
            if (!thisToThatNameMap.containsKey(thisColumnName)) {
                rtn.add(-1);
            } else {
                rtn.add(thatColumnNames.indexOf(thisToThatNameMap.get(thisColumnName)));
            }
        }
        if (logDebug)
            logger.debug("Corresponding index for {}: {}-{}", isOrigin ? "origin" : "target", columnNames, rtn);
        return rtn;
    }

    // This facilitates unit testing
    protected Metadata fetchMetadataFromSession(CqlSession cqlSession) {
        return cqlSession.getMetadata();
    }

    private void setCqlMetadata(CqlSession cqlSession) {
        Metadata metadata = fetchMetadataFromSession(cqlSession);

        String partitionerName = metadata.getTokenMap().get().getPartitionerName();
        if (null != partitionerName && partitionerName.endsWith("RandomPartitioner"))
            this.hasRandomPartitioner = true;
        else
            this.hasRandomPartitioner = false;

        Optional<KeyspaceMetadata> keyspaceMetadataOpt = metadata.getKeyspace(formatName(this.keyspaceName));
        if (!keyspaceMetadataOpt.isPresent()) {
            throw new IllegalArgumentException("Keyspace not found: " + this.keyspaceName);
        }
        KeyspaceMetadata keyspaceMetadata = keyspaceMetadataOpt.get();

        Optional<TableMetadata> tableMetadataOpt = keyspaceMetadata.getTable(formatName(this.tableName));
        if (!tableMetadataOpt.isPresent()) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        TableMetadata tableMetadata = tableMetadataOpt.get();

        this.cqlPrimaryKey = new ArrayList<>();
        this.cqlAllColumns = new ArrayList<>();

        this.cqlPartitionKey = tableMetadata.getPartitionKey();
        this.cqlPrimaryKey.addAll(this.cqlPartitionKey);
        this.cqlPrimaryKey.addAll(tableMetadata.getClusteringColumns().keySet());
        this.cqlAllColumns.addAll(this.cqlPrimaryKey);

        boolean extractJsonExclusive = propertyHelper.getBoolean(KnownProperties.EXTRACT_JSON_EXCLUSIVE);
        String extractColumnName = "";
        if (extractJsonExclusive) {
            String originColumnName = unFormatName(
                    propertyHelper.getString(KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME));
            String targetColumnName = unFormatName(
                    propertyHelper.getString(KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING));
            extractColumnName = isOrigin ? originColumnName : targetColumnName;
            if (!extractColumnName.isBlank() && extractColumnName.contains(":")) {
                extractColumnName = extractColumnName.split("\\:")[1];
            }
        }
        String columnName = extractColumnName;
        List<String> skipColumns = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_SKIP);
        this.cqlAllColumns = tableMetadata.getColumns().values().stream().filter(md -> !this.cqlAllColumns.contains(md))
                .filter(md -> CollectionUtils.isEmpty(skipColumns) || !skipColumns.contains(md.getName().asCql(true)))
                .filter(md -> !extractJsonExclusive || md.getName().asCql(true).endsWith(columnName))
                .collect(Collectors.toCollection(() -> this.cqlAllColumns));

        boolean allowCollectionsForWritetimeTTL = propertyHelper
                .getBoolean(KnownProperties.ALLOW_COLL_FOR_WRITETIME_TTL_CALC);
        this.writetimeTTLColumns = tableMetadata.getColumns().values().stream()
                .filter(columnMetadata -> canColumnHaveTTLorWritetime(tableMetadata, columnMetadata,
                        allowCollectionsForWritetimeTTL))
                .map(ColumnMetadata::getName).map(CqlIdentifier::asInternal).collect(Collectors.toList());

        this.columnNameToCqlTypeMap = this.cqlAllColumns.stream().collect(
                Collectors.toMap(columnMetadata -> columnMetadata.getName().asInternal(), ColumnMetadata::getType));
    }

    private boolean canColumnHaveTTLorWritetime(TableMetadata tableMetadata, ColumnMetadata columnMetadata,
            boolean allowCollectionsForWritetimeTTL) {
        DataType dataType = columnMetadata.getType();
        boolean isKeyColumn = tableMetadata.getPartitionKey().contains(columnMetadata)
                || tableMetadata.getClusteringColumns().containsKey(columnMetadata);

        if (isKeyColumn)
            return false;
        if (CqlData.isPrimitive(dataType))
            return true;
        if (dataType instanceof TupleType)
            return true; // TODO: WRITETIME and TTL functions are very slow on Tuples in cqlsh...should they be
                         // supported here?
        if (CqlData.isFrozen(dataType))
            return true;
        if (allowCollectionsForWritetimeTTL && CqlData.isCollection(dataType))
            return true;
        return false;
    }

    public List<String> getWritetimeTTLColumns() {
        return this.writetimeTTLColumns.stream().filter(columnName -> this.columnNames.contains(columnName))
                .collect(Collectors.toList());
    }

    public boolean isWritetimeTTLColumn(String columnName) {
        return this.writetimeTTLColumns.contains(columnName);
    }

    public boolean hasUnfrozenList() {
        return this.cqlAllColumns.stream()
                .filter(columnMetadata -> columnNames.contains(columnMetadata.getName().asInternal())
                        && columnMetadata.getType() instanceof ListType)
                .anyMatch(columnMetadata -> !CqlData.isFrozen(columnMetadata.getType()));
    }

    protected static ConsistencyLevel mapToConsistencyLevel(String level) {
        ConsistencyLevel retVal = ConsistencyLevel.LOCAL_QUORUM;
        if (StringUtils.isNotEmpty(level)) {
            switch (level.toUpperCase()) {
            case "ANY":
                retVal = ConsistencyLevel.ANY;
                break;
            case "ONE":
                retVal = ConsistencyLevel.ONE;
                break;
            case "TWO":
                retVal = ConsistencyLevel.TWO;
                break;
            case "THREE":
                retVal = ConsistencyLevel.THREE;
                break;
            case "QUORUM":
                retVal = ConsistencyLevel.QUORUM;
                break;
            case "LOCAL_ONE":
                retVal = ConsistencyLevel.LOCAL_ONE;
                break;
            case "EACH_QUORUM":
                retVal = ConsistencyLevel.EACH_QUORUM;
                break;
            case "SERIAL":
                retVal = ConsistencyLevel.SERIAL;
                break;
            case "LOCAL_SERIAL":
                retVal = ConsistencyLevel.LOCAL_SERIAL;
                break;
            case "ALL":
                retVal = ConsistencyLevel.ALL;
                break;
            }
        }

        return retVal;
    }

}
