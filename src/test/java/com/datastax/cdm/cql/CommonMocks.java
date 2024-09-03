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
package com.datastax.cdm.cql;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.cdm.data.*;
import com.datastax.cdm.data.Record;
import com.datastax.cdm.feature.*;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;

/**
 * This class is a bit bonkers, to be honest. It is basically trying to provide a simulation of Cassandra plus some of
 * the other classes in the CDM codebase. It is used by the unit tests, and originally designed for statements but it
 * could well be useful in other contexts as well.
 */
public class CommonMocks {
    private boolean hasExplodeMap;
    private boolean hasConstantColumns;
    private boolean hasCounters;

    @Mock
    public IPropertyHelper propertyHelper;

    @Mock
    public EnhancedSession originSession;
    @Mock
    public CqlSession originCqlSession;
    @Mock
    public CqlTable originTable;
    @Mock
    public ResultSet originResultSet;
    @Mock
    public MutableCodecRegistry originCodecRegistry;
    @Mock
    public TypeCodec originCodec;
    @Mock
    public List<CqlConversion> originConversionList;
    @Mock
    public CqlConversion originCqlConversion;

    @Mock
    public EnhancedSession targetSession;
    @Mock
    public CqlSession targetCqlSession;
    @Mock
    public CqlTable targetTable;
    @Mock
    public ResultSet targetResultSet;
    @Mock
    public MutableCodecRegistry targetCodecRegistry;
    @Mock
    public TypeCodec targetCodec;
    @Mock
    public List<CqlConversion> targetConversionList;
    @Mock
    public CqlConversion targetCqlConversion;

    @Mock
    public ConstantColumns constantColumnsFeature;
    @Mock
    public ExplodeMap explodeMapFeature;
    @Mock
    public ExtractJson extractJsonFeature;
    @Mock
    public WritetimeTTL writetimeTTLFeature;
    @Mock
    public OriginFilterCondition originFilterConditionFeature;

    @Mock
    public PreparedStatement preparedStatement;
    @Mock
    public BoundStatement boundStatement;
    @Mock
    public CompletionStage<AsyncResultSet> completionStage;

    @Mock
    public EnhancedPK pk;
    @Mock
    public PKFactory pkFactory;
    @Mock
    public Record record;
    @Mock
    public Row originRow;
    @Mock
    public Row targetRow;

    public String originKeyspaceName;
    public String originTableName;
    public String originKeyspaceTableName;
    public List<String> originPartitionKey;
    public List<DataType> originPartitionKeyTypes;
    public List<String> originClusteringKey;
    public List<DataType> originClusteringKeyTypes;
    public String filterCol;
    public DataType filterColType;
    public String vectorCol;
    public DataType vectorColType;
    public List<String> originValueColumns;
    public List<DataType> originValueColumnTypes;
    public List<String> originCounterColumns;
    public List<Integer> originCounterIndexes;
    public List<String> originPrimaryKey;
    public List<String> originColumnNames;
    public List<DataType> originColumnTypes;

    public List<String> originToTargetNameList;

    public String targetKeyspaceName;
    public String targetTableName;
    public String targetKeyspaceTableName;
    public List<String> targetPartitionKey;
    public List<DataType> targetPartitionKeyTypes;
    public List<String> targetClusteringKey;
    public List<DataType> targetClusteringKeyTypes;
    public List<String> targetValueColumns;
    public List<DataType> targetValueColumnTypes;
    public List<String> targetCounterColumns;
    public List<Integer> targetCounterIndexes;
    public List<String> targetPrimaryKey;
    public List<String> targetColumnNames;
    public List<DataType> targetColumnTypes;

    public String minPartition;
    public String maxPartition;
    public ConsistencyLevel readCL;
    public Integer fetchSizeInRows;

    public String explodeMapColumn;
    public String explodeMapKey;
    public DataType explodeMapKeyType;
    public String explodeMapValue;
    public DataType explodeMapValueType;
    public DataType explodeMapType;

    public List<String> constantColumns;
    public List<String> constantColumnValues;
    public List<DataType> constantColumnTypes;

    public void commonSetup() {
        commonSetup(false, false, false);
    }

    public void commonSetup(boolean hasExplodeMap, boolean hasConstantColumns, boolean hasCounters) {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables(hasExplodeMap, hasConstantColumns, hasCounters);
    }

    public void commonSetupWithoutDefaultClassVariables(boolean hasExplodeMap, boolean hasConstantColumns,
            boolean hasCounters) {
        if (hasCounters && (hasExplodeMap || hasConstantColumns)) {
            throw new IllegalArgumentException("Counters cannot be used with ExplodeMap or ConstantColumns");
        }

        this.hasExplodeMap = hasExplodeMap;
        this.hasConstantColumns = hasConstantColumns;
        this.hasCounters = hasCounters;

        MockitoAnnotations.openMocks(this);
        setCompoundClassVariables();
        setPropertyHelperWhens();

        setOriginVariables();
        setOriginTableWhens();
        setSessionWhens(true); // origin

        setTargetVariables();
        setTargetTableWhens();
        setSessionWhens(false); // target

        setPKAndRecordWhens();
        setStatementAndBindWhens();
    }

    public void commonSetupWithoutDefaultClassVariables() {
        commonSetupWithoutDefaultClassVariables(false, false, false);
    }

    public void defaultClassVariables() {
        originKeyspaceName = "origin_ks";
        originTableName = "table_name";
        originPartitionKey = Arrays.asList("part_key1", "part_key2");
        originPartitionKeyTypes = Arrays.asList(DataTypes.TEXT, DataTypes.TEXT);
        originClusteringKey = Collections.singletonList("cluster_key");
        originClusteringKeyTypes = Collections.singletonList(DataTypes.TEXT);
        filterCol = "filter_col";
        filterColType = DataTypes.TEXT;
        vectorCol = "vector_col";
        vectorColType = DataTypes.vectorOf(DataTypes.FLOAT, 3);
        originValueColumns = Arrays.asList("value1", filterCol, vectorCol);
        originValueColumnTypes = Arrays.asList(DataTypes.TEXT, filterColType, vectorColType);
        originCounterColumns = Arrays.asList("counter1", "counter2");
        originToTargetNameList = Collections.emptyList();

        targetKeyspaceName = "target_ks";
        targetTableName = "table_name";

        minPartition = "-9876543";
        maxPartition = "1234567";
        readCL = ConsistencyLevel.LOCAL_QUORUM;
        fetchSizeInRows = 999;

        explodeMapColumn = "explode_map";
        explodeMapKey = "map_key";
        explodeMapKeyType = DataTypes.TEXT;
        explodeMapValue = "map_value";
        explodeMapValueType = DataTypes.TEXT;

        constantColumns = Arrays.asList("const1", "const2", "const3");
        constantColumnValues = Arrays.asList("'abcd'", "1234", "543");
        constantColumnTypes = Arrays.asList(DataTypes.TEXT, DataTypes.INT, DataTypes.BIGINT);
    }

    // This method for variables that are composed of other class variables.
    public void setCompoundClassVariables() {
        originKeyspaceTableName = originKeyspaceName + "." + originTableName;
        targetKeyspaceTableName = targetKeyspaceName + "." + targetTableName;
        explodeMapType = DataTypes.mapOf(explodeMapKeyType, explodeMapValueType);
    }

    public void setOriginVariables() {
        originPrimaryKey = new ArrayList<>(originPartitionKey);
        originPrimaryKey.addAll(originClusteringKey);

        originColumnNames = new ArrayList<>(originPrimaryKey);
        if (hasCounters) {
            originColumnNames.addAll(originCounterColumns);
            originCounterIndexes = originCounterColumns.stream().map(originColumnNames::indexOf)
                    .collect(Collectors.toList());

        } else {
            originColumnNames.addAll(originValueColumns);
            originCounterIndexes = Collections.emptyList();
        }

        originColumnTypes = new ArrayList<>(originPartitionKeyTypes);
        originColumnTypes.addAll(originClusteringKeyTypes);
        if (hasCounters) {
            originColumnTypes.addAll(Collections.nCopies(originCounterColumns.size(), DataTypes.COUNTER));
        } else {
            originColumnTypes.addAll(originValueColumnTypes);
        }

        if (hasExplodeMap) {
            if (!originValueColumns.contains(explodeMapColumn)) {
                originColumnNames.add(explodeMapColumn);
                originColumnTypes.add(explodeMapType);
            }
        }
    }

    public void setPropertyHelperWhens() {
        when(propertyHelper.getAsString(KnownProperties.FILTER_COLUMN_NAME)).thenReturn("");
        when(propertyHelper.getString(KnownProperties.FILTER_COLUMN_VALUE)).thenReturn(null);

        when(propertyHelper.getString(KnownProperties.PARTITION_MIN)).thenReturn(minPartition);
        when(propertyHelper.getString(KnownProperties.PARTITION_MAX)).thenReturn(maxPartition);

        when(propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET)).thenReturn(originToTargetNameList);
    }

    public void setOriginTableWhens() {
        when(originTable.getKeyspaceName()).thenReturn(originKeyspaceName);
        when(originTable.getTableName()).thenReturn(originTableName);
        when(originTable.getKeyspaceTable()).thenReturn(originKeyspaceTableName);
        when(originTable.isOrigin()).thenReturn(true);
        when(originTable.getColumnNames(false)).thenReturn(originColumnNames);
        when(originTable.getColumnNames(true)).thenReturn(CqlTable.formatNames(originColumnNames));
        when(originTable.indexOf(anyString()))
                .thenAnswer(invocation -> {
                    String name = invocation.getArgument(0, String.class);
                    return originColumnNames.indexOf(name);
                });
        when(originTable.getColumnCqlTypes()).thenReturn(originColumnTypes);
        when(originTable.getDataType(anyString())).thenAnswer(invocation -> {String name = invocation.getArgument(0,String.class); return originColumnTypes.get(originColumnNames.indexOf(name));});
        when(originTable.getWritetimeTTLColumns()).thenReturn(originValueColumns);

        when(originTable.getOtherCqlTable()).thenReturn(targetTable);
        when(originTable.getCorrespondingIndex(anyInt())).thenAnswer(invocation -> targetColumnNames.indexOf(originColumnNames.get(invocation.getArgument(0, Integer.class))));
        when(originTable.getBindClass(anyInt()))
                .thenAnswer(invocation -> {
                    int index = invocation.getArgument(0, Integer.class);
                    return CqlData.getBindClass(originColumnTypes.get(index));
                });
        when(originTable.getCounterIndexes()).thenReturn(originCounterIndexes);
        when(originTable.getData(anyInt(), eq(originRow))).thenAnswer(invocation -> getSampleData(originColumnTypes.get(invocation.getArgument(0, Integer.class))));
        when(originTable.getAndConvertData(anyInt(), eq(originRow))).thenAnswer(invocation -> getSampleData(originColumnTypes.get(invocation.getArgument(0, Integer.class))));
        when(pkFactory.getWhereClause(PKFactory.Side.ORIGIN)).thenReturn(keyEqualsBindJoinedWithAND(CqlTable.formatNames(originPrimaryKey)));

        when(originTable.getFeature(Featureset.EXPLODE_MAP)).thenReturn(explodeMapFeature);
        when(originTable.getFeature(Featureset.EXTRACT_JSON)).thenReturn(extractJsonFeature);
        when(originTable.getFeature(Featureset.CONSTANT_COLUMNS)).thenReturn(constantColumnsFeature);

        when(originTable.getFeature(Featureset.WRITETIME_TTL)).thenReturn(writetimeTTLFeature);
        when(writetimeTTLFeature.isEnabled()).thenReturn(false);

        when(originTable.getFeature(Featureset.ORIGIN_FILTER)).thenReturn(originFilterConditionFeature);
        when(originFilterConditionFeature.isEnabled()).thenReturn(false);
        when(originFilterConditionFeature.getFilterCondition()).thenReturn("");

        when(originTable.getCodecRegistry()).thenReturn(originCodecRegistry);
        when(originCodecRegistry.codecFor(any(DataType.class), any(Class.class))).thenReturn(originCodec);
        when(originCodecRegistry.codecFor(any(DataType.class))).thenReturn(originCodec);

        when(originTable.getConversions()).thenReturn(originConversionList);
        when(originConversionList.get(anyInt())).thenReturn(originCqlConversion);
        when(originCqlConversion.convert(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    public void setTargetVariables() {
        if (null == targetPartitionKey || targetPartitionKey.isEmpty())
            targetPartitionKey = new ArrayList<>(originPartitionKey);
        if (null == targetClusteringKey || targetClusteringKey.isEmpty())
            targetClusteringKey = new ArrayList<>(originClusteringKey);
        if (null == targetValueColumns || targetValueColumns.isEmpty())
            targetValueColumns = new ArrayList<>(originValueColumns);
        if (null == targetPartitionKeyTypes || targetPartitionKeyTypes.isEmpty())
            targetPartitionKeyTypes = new ArrayList<>(originPartitionKeyTypes);
        if (null == targetClusteringKeyTypes || targetClusteringKeyTypes.isEmpty())
            targetClusteringKeyTypes = new ArrayList<>(originClusteringKeyTypes);
        if (null == targetValueColumnTypes || targetValueColumnTypes.isEmpty())
            targetValueColumnTypes = new ArrayList<>(originValueColumnTypes);
        if (null == targetCounterColumns || targetCounterColumns.isEmpty())
            targetCounterColumns = new ArrayList<>(originCounterColumns);

        targetPrimaryKey = new ArrayList<>(targetPartitionKey);
        targetPrimaryKey.addAll(targetClusteringKey);

        targetColumnNames = new ArrayList<>(targetPrimaryKey);
        if (hasCounters) {
            targetColumnNames.addAll(targetCounterColumns);
            targetCounterIndexes = targetCounterColumns.stream().map(targetColumnNames::indexOf)
                    .collect(Collectors.toList());
        } else {
            targetColumnNames.addAll(targetValueColumns);
            targetCounterIndexes = Collections.emptyList();
        }

        targetColumnTypes = new ArrayList<>(targetPartitionKeyTypes);
        targetColumnTypes.addAll(targetClusteringKeyTypes);
        if (hasCounters) {
            targetColumnTypes.addAll(Collections.nCopies(targetCounterColumns.size(), DataTypes.COUNTER));
        } else {
            targetColumnTypes.addAll(targetValueColumnTypes);
        }

        if (hasExplodeMap) {
            if (!targetColumnNames.contains(explodeMapKey)) {
                targetColumnNames.add(explodeMapKey);
                targetColumnTypes.add(explodeMapKeyType);
            }
            if (!targetPrimaryKey.contains(explodeMapKey)) {
                targetPrimaryKey.add(explodeMapKey);
            }
            if (!targetColumnNames.contains(explodeMapValue)) {
                targetColumnNames.add(explodeMapValue);
                targetColumnTypes.add(explodeMapValueType);
            }
        }

        if (hasConstantColumns) {
            for (String constantColumn : constantColumns) {
                if (!targetColumnNames.contains(constantColumn)) {
                    targetColumnNames.add(constantColumn);
                    targetColumnTypes.add(constantColumnTypes.get(constantColumns.indexOf(constantColumn)));
                }
            }
        }
    }

    public void setTargetTableWhens() {
        when(targetTable.getKeyspaceName()).thenReturn(targetKeyspaceName);
        when(targetTable.getTableName()).thenReturn(targetTableName);
        when(targetTable.getKeyspaceTable()).thenReturn(targetKeyspaceTableName);
        when(targetTable.isOrigin()).thenReturn(false);
        when(targetTable.getColumnNames(false)).thenReturn(targetColumnNames);
        when(targetTable.getColumnNames(true)).thenReturn(CqlTable.formatNames(targetColumnNames));
        when(targetTable.indexOf(anyString()))
                .thenAnswer(invocation -> {
                    String name = invocation.getArgument(0, String.class);
                    return targetColumnNames.indexOf(name);
                });
        when(targetTable.getColumnCqlTypes()).thenReturn(targetColumnTypes);
        when(targetTable.getDataType(anyString())).thenAnswer(invocation -> {String name = invocation.getArgument(0,String.class); return targetColumnTypes.get(targetColumnNames.indexOf(name));});
        when(targetTable.getOtherCqlTable()).thenReturn(originTable);
        when(targetTable.getCorrespondingIndex(anyInt())).thenAnswer(invocation -> originColumnNames.indexOf(targetColumnNames.get(invocation.getArgument(0, Integer.class))));
        when(targetTable.getBindClass(anyInt()))
                .thenAnswer(invocation -> {
                    int index = invocation.getArgument(0, Integer.class);
                    return CqlData.getBindClass(targetColumnTypes.get(index));
                });
        when(targetTable.getCounterIndexes()).thenReturn(targetCounterIndexes);
        when(targetTable.getData(anyInt(), eq(targetRow))).thenAnswer(invocation -> getSampleData(targetColumnTypes.get(invocation.getArgument(0, Integer.class))));
        when(targetTable.getAndConvertData(anyInt(), eq(targetRow))).thenAnswer(invocation -> getSampleData(targetColumnTypes.get(invocation.getArgument(0, Integer.class))));
        when(pkFactory.getWhereClause(PKFactory.Side.TARGET)).thenReturn(keyEqualsBindJoinedWithAND(CqlTable.formatNames(targetPrimaryKey)));

        when(targetTable.getFeature(Featureset.CONSTANT_COLUMNS)).thenReturn(constantColumnsFeature);
        if (hasConstantColumns) {
            when(constantColumnsFeature.isEnabled()).thenReturn(true);
            when(constantColumnsFeature.getNames()).thenReturn(constantColumns);
            when(constantColumnsFeature.getValues()).thenReturn(constantColumnValues);
            when(constantColumnsFeature.getBindClasses()).thenReturn(constantColumnTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList()));
            when(targetTable.extendColumns(constantColumns)).thenReturn(constantColumnTypes.stream().map(CqlData::getBindClass).collect(Collectors.toList()));
        } else {
            when(constantColumnsFeature.isEnabled()).thenReturn(false);
            when(constantColumnsFeature.getNames()).thenReturn(Collections.emptyList());
            when(constantColumnsFeature.getValues()).thenReturn(Collections.emptyList());
            when(constantColumnsFeature.getBindClasses()).thenReturn(Collections.emptyList());
        }

        when(targetTable.getFeature(Featureset.EXPLODE_MAP)).thenReturn(explodeMapFeature);
        if (hasExplodeMap) {
            when(explodeMapFeature.isEnabled()).thenReturn(true);
            when(explodeMapFeature.getOriginColumnIndex()).thenReturn(originColumnNames.indexOf(explodeMapColumn));
            when(explodeMapFeature.getKeyColumnIndex()).thenReturn(targetColumnNames.indexOf(explodeMapKey));
            when(explodeMapFeature.getValueColumnIndex()).thenReturn(targetColumnNames.indexOf(explodeMapValue));
            when(explodeMapFeature.getOriginColumnName()).thenReturn(explodeMapColumn);
            when(explodeMapFeature.getKeyColumnName()).thenReturn(explodeMapKey);
            when(explodeMapFeature.getValueColumnName()).thenReturn(explodeMapValue);
        } else {
            when(explodeMapFeature.isEnabled()).thenReturn(false);
            when(explodeMapFeature.getKeyColumnIndex()).thenReturn(-1);
            when(explodeMapFeature.getValueColumnIndex()).thenReturn(-1);
        }

        when(targetTable.getFeature(Featureset.EXTRACT_JSON)).thenReturn(extractJsonFeature);
        when(extractJsonFeature.isEnabled()).thenReturn(false);
        when(extractJsonFeature.getOriginColumnIndex()).thenReturn(-1);
        when(extractJsonFeature.getTargetColumnIndex()).thenReturn(-1);

        when(targetTable.getFeature(Featureset.WRITETIME_TTL)).thenReturn(writetimeTTLFeature);
        when(writetimeTTLFeature.isEnabled()).thenReturn(false);

        when(targetTable.getCodecRegistry()).thenReturn(targetCodecRegistry);
        when(targetCodecRegistry.codecFor(any(DataType.class), any(Class.class))).thenReturn(targetCodec);
        when(targetCodecRegistry.codecFor(any(DataType.class))).thenReturn(targetCodec);

        when(targetTable.getConversions()).thenReturn(targetConversionList);
        when(targetConversionList.get(anyInt())).thenReturn(targetCqlConversion);
        when(targetCqlConversion.convert(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    public void setSessionWhens(boolean isOrigin) {
        EnhancedSession session = isOrigin ? originSession : targetSession;
        CqlTable cqlTable = isOrigin ? originTable : targetTable;
        CqlSession cqlSession = isOrigin ? originCqlSession : targetCqlSession;
        Row localRow = isOrigin ? originRow : targetRow;
        ResultSet localResultSet = isOrigin ? originResultSet : targetResultSet;
        List<String> primaryKeys = isOrigin ? originPrimaryKey : targetPrimaryKey;
        List<String> partitionKeys = isOrigin ? originPartitionKey : targetPartitionKey;
        PKFactory.Side side = isOrigin ? PKFactory.Side.ORIGIN : PKFactory.Side.TARGET;

        when(session.getCqlTable()).thenReturn(cqlTable);
        when(session.getCqlSession()).thenReturn(cqlSession);
        when(session.getPKFactory()).thenReturn(pkFactory);
        when(pkFactory.getPKNames(eq(side), eq(false))).thenReturn(primaryKeys);
        when(pkFactory.getPKNames(eq(side), eq(true))).thenReturn(CqlTable.formatNames((primaryKeys)));

        when(cqlTable.getReadConsistencyLevel()).thenReturn(readCL);
        when(cqlTable.getFetchSizeInRows()).thenReturn(fetchSizeInRows);
        when(cqlTable.hasRandomPartitioner()).thenReturn(false);
        when(cqlTable.getPartitionKeyNames(false)).thenReturn(partitionKeys);
        when(cqlTable.getPartitionKeyNames(true)).thenReturn(CqlTable.formatNames(partitionKeys));

        when(cqlSession.prepare(anyString())).thenReturn(preparedStatement);
        when(cqlSession.executeAsync(any(Statement.class))).thenReturn(completionStage);
        when(cqlSession.execute(any(BoundStatement.class))).thenReturn(localResultSet);
        when(cqlSession.execute(boundStatement)).thenReturn(localResultSet);

        when(localResultSet.one()).thenReturn(localRow);
    }

    public void setStatementAndBindWhens() {
        when(preparedStatement.bind()).thenReturn(boundStatement);
        when(preparedStatement.bind(anyLong(), anyLong())).thenReturn(boundStatement);
        when(preparedStatement.bind(any(BigInteger.class), any(BigInteger.class))).thenReturn(boundStatement);

        when(boundStatement.set(anyInt(), any(), any(Class.class))).thenReturn(boundStatement);
        when(boundStatement.setConsistencyLevel(any())).thenReturn(boundStatement);
        when(boundStatement.setTimeout(any())).thenReturn(boundStatement);
        when(boundStatement.setPageSize(any(Integer.class))).thenReturn(boundStatement);

        when(pkFactory.bindWhereClause(any(PKFactory.Side.class), any(EnhancedPK.class), eq(boundStatement), anyInt())).thenReturn(boundStatement);
    }

    public void setPKAndRecordWhens() {
        when(record.getPk()).thenReturn(pk);
        when(record.getOriginRow()).thenReturn(originRow);
        when(record.getTargetRow()).thenReturn(targetRow);
        when(pkFactory.getTargetPK(originRow)).thenReturn(pk);
    }

    public static Object getSampleData(DataType type) {
        CqlData.Type cqlDataType = CqlData.toType(type);
        switch (cqlDataType) {
        case PRIMITIVE:
            if (type.equals(DataTypes.BOOLEAN))
                return true;
            if (type.equals(DataTypes.TINYINT))
                return (byte) 1;
            if (type.equals(DataTypes.SMALLINT))
                return (short) 1;
            if (type.equals(DataTypes.INT))
                return 1;
            if (type.equals(DataTypes.BIGINT))
                return 1L;
            if (type.equals(DataTypes.FLOAT))
                return 1.0f;
            if (type.equals(DataTypes.DOUBLE))
                return 1.0d;
            if (type.equals(DataTypes.DECIMAL))
                return new BigDecimal(1);
            if (type.equals(DataTypes.UUID))
                return UUID.randomUUID();
            if (type.equals(DataTypes.INET))
                return InetAddress.getLoopbackAddress();
            if (type.equals(DataTypes.TIMESTAMP))
                return Instant.now();
            if (type.equals(DataTypes.TIME))
                return LocalTime.now();
            if (type.equals(DataTypes.DATE))
                return LocalDate.now();
            if (type.equals(DataTypes.DURATION))
                return Duration.ofSeconds(1);
            if (type.equals(DataTypes.BLOB))
                return ByteBuffer.wrap("sample_data".getBytes());
            if (type.equals(DataTypes.ASCII))
                return "sample_data";
            if (type.equals(DataTypes.TEXT))
                return "sample_data";
            if (type.equals(DataTypes.VARINT))
                return BigInteger.ONE;
            if (type.equals(DataTypes.COUNTER))
                return 1L;
            if (type.equals(DataTypes.TIMEUUID))
                return UUID.randomUUID();
            break;
        case LIST:
            return Arrays.asList("1", "2", "3");
        case SET:
            return new HashSet(Arrays.asList("1", "2", "3"));
        case MAP:
            return new HashMap<String, String>() {
                {
                    put("1", "one");
                    put("2", "two");
                    put("3", "three");
                }
            };
        case VECTOR:
            return CqlVector.newInstance(1.1, 2.2, 3.3);
        }
        return "DataType " + type + " is not supported, so returning a String";
    }

    public String keyEqualsBindJoinedWithAND(List<String> bindList) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bindList.size(); i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            String key = bindList.get(i);
            if (constantColumns.contains(key)) {
                sb.append(key).append("=").append(constantColumnValues.get(constantColumns.indexOf(key)));
            } else {
                sb.append(key).append("=?");
            }
        }
        return sb.toString();
    }
}
