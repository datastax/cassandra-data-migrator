package datastax.cdm.feature;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * This Feature is currently able to map a UDT to another UDT, on the assumption
 * that each field in the origin UDT has a corresponding field in the target UDT,
 * and in the same position. It also assumes the field types are the same, or
 * at least have a codec pairing that is able to convert between the two.
 *
 * It handles the following data types:
 *    UdtValue
 *    List<UdtValue>
 *    Set<UdtValue>
 *    Map<Object,UdtValue>
 *    Map<UdtValue,Object>
 *    Map<UdtValue,UdtValue>
 *
 */
public class UDTMapper extends AbstractFeature {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private List<UserDefinedType> originUDTList = new ArrayList<>();
    private List<UserDefinedType> targetUDTList = new ArrayList<>();
    private List<Tuple2<UserDefinedType,UserDefinedType>> fromOriginToTargetList = new ArrayList<>();
    private List<Tuple2<UserDefinedType,UserDefinedType>> fromTargetToOriginList = new ArrayList<>();

    @Override
    public boolean initialize(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        this.originUDTList = getUDTList(true, propertyHelper, cqlHelper);
        this.targetUDTList = getUDTList(false, propertyHelper, cqlHelper);
        this.fromOriginToTargetList = getFromToList(true, propertyHelper);
        this.fromTargetToOriginList = getFromToList(false, propertyHelper);

        // we are enabled if the UDTList has any non-null entries
        for (UserDefinedType udt : originUDTList) {
            if (null!=udt) {
                isEnabled = true;
                break;
            }
        }
        if (isEnabled)
            logger.info("UDTMapper is enabled, with origin columns: "+this.fromOriginToTargetList);

        isInitialized = true;
        return isInitialized;
    }

    public Object convert(boolean isOrigin, int columnIndex, Object object) {
        if (null==object) {
            return null;
        }
        if (isOrigin && null!=fromOriginToTargetList.get(columnIndex)) {
            return convert(object, fromOriginToTargetList.get(columnIndex));
        }
        else if (!isOrigin && null!=fromTargetToOriginList.get(columnIndex)){
            return convert(object, fromTargetToOriginList.get(columnIndex));
        }
        // no conversion needed
        return object;
    }

    public static UdtValue map(UserDefinedType fromUDT, UdtValue fromUDTValue, UserDefinedType toUDT) {
        List<DataType> fromFieldTypes = fromUDT.getFieldTypes();
        List<DataType> toFieldTypes = toUDT.getFieldTypes();
        if (null==fromFieldTypes || null==toFieldTypes || fromFieldTypes.size() != toFieldTypes.size()) {
            throw new IllegalArgumentException("fromUDT and toUDT not be null and must have the same number of fields");
        }
        if (!fromUDTValue.getType().getClass().equals(fromUDT.getClass())) {
            throw new IllegalArgumentException("fromUDT and fromUDTValue must be of the same type");
        }

        UdtValue toUDTValue = toUDT.newValue();

        int loopLimit = fromFieldTypes.size();
        for (int i = 0; i < loopLimit; i++) {
            DataType fromFieldType = fromFieldTypes.get(i);
            TypeCodec<Object> fromCodec = fromUDT.getAttachmentPoint().getCodecRegistry().codecFor(fromFieldType);

            DataType toFieldType = toFieldTypes.get(i);
            TypeCodec<Object> toCodec = toUDT.getAttachmentPoint().getCodecRegistry().codecFor(toFieldType);

            Object fromFieldValue = fromUDTValue.get(i, fromCodec);
            Object toFieldValue = toCodec.parse(fromCodec.format(fromFieldValue));
            toUDTValue.set(i, toFieldValue, toCodec);
        }

        return toUDTValue;
    }

    public static Object convert(Object object, Tuple2<UserDefinedType,UserDefinedType> fromToUdt) {
        if (null==object || null == fromToUdt) {
            return null;
        }
        UserDefinedType fromUDT = fromToUdt._1();
        UserDefinedType toUDT = fromToUdt._2();
        if (null == fromUDT || null == toUDT) {
            return null;
        }

        if (object instanceof UdtValue) {
            return map(fromUDT, (UdtValue)object, toUDT);
        }
        else if (object instanceof List) {
            List<UdtValue> fromUDTValues = (List<UdtValue>)object;
            List<UdtValue> toUDTValues = new ArrayList<>();
            for (UdtValue fromUDTValue : fromUDTValues) {
                toUDTValues.add(map(fromUDT, fromUDTValue, toUDT));
            }
            return toUDTValues;
        }
        else if (object instanceof Set) {
            Set<UdtValue> fromUDTValues = (Set<UdtValue>)object;
            Set<UdtValue> toUDTValues = new HashSet<>();
            for (UdtValue fromUDTValue : fromUDTValues) {
                toUDTValues.add(map(fromUDT, fromUDTValue, toUDT));
            }
            return toUDTValues;
        }
        else if (object instanceof Map) {
            Map<Object,Object> fromMap = (Map<Object,Object>)object;
            if (fromMap.isEmpty()) {
                return object;
            }
            Map.Entry<Object, Object> oneEntry = fromMap.entrySet().iterator().next();
            boolean shouldMapKey = oneEntry.getKey() instanceof UdtValue;
            boolean shouldMapValue = oneEntry.getValue() instanceof UdtValue;

            Map<Object, Object> toMap = new HashMap<>();
            for (Map.Entry<Object,Object> entry : fromMap.entrySet()) {
                Object key = shouldMapKey ? map(fromUDT, (UdtValue)entry.getKey(), toUDT) : entry.getKey();
                Object value = shouldMapValue ? map(fromUDT, (UdtValue)entry.getValue(), toUDT) : entry.getValue();
                toMap.put(key, value);
            }
            return toMap;
        }
        else {
            throw new IllegalArgumentException("Object must be of type UdtValue, List<UdtValue>, Set<UdtValue>, Map<Object,UdtValue>, Map<UdtValue,Object>, or Map<UdtValue,UdtValue>");
        }
    }

    // Make a List that has as many elements as there are columns in the table
    // If the column has a UDT, then the element will be the UDT, otherwise it will be null
    private List<UserDefinedType> getUDTList(boolean fromOrigin, PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        List<UserDefinedType> udtList = new ArrayList<>();
        List<MigrateDataType> dataTypeList = fromOrigin ? ColumnsKeysTypes.getOriginColumnTypes(propertyHelper) : ColumnsKeysTypes.getTargetColumnTypes(propertyHelper);
        List<String> columnNameList = fromOrigin ? ColumnsKeysTypes.getOriginColumnNames(propertyHelper) : ColumnsKeysTypes.getTargetColumnNames(propertyHelper);
        String keyspace = fromOrigin ? ColumnsKeysTypes.getOriginKeyspace(propertyHelper) : ColumnsKeysTypes.getTargetKeyspace(propertyHelper);
        String table = fromOrigin ? ColumnsKeysTypes.getOriginTable(propertyHelper) : ColumnsKeysTypes.getTargetTable(propertyHelper);

        for (int i=0; i< dataTypeList.size(); i++) {
            String columnName = columnNameList.get(i);
            MigrateDataType dataType = dataTypeList.get(i);

            if (dataType.hasUDT()) {
                Session session = fromOrigin ? cqlHelper.getOriginSessionInit() : cqlHelper.getTargetSessionInit();
                Metadata metadata = session.getMetadata();

                Optional<KeyspaceMetadata> keyspaceMetadataOpt = metadata.getKeyspace(keyspace);
                if (!keyspaceMetadataOpt.isPresent()) {
                    throw new IllegalArgumentException("Keyspace not found: " + keyspace);
                }
                KeyspaceMetadata keyspaceMetadata = keyspaceMetadataOpt.get();

                Optional<TableMetadata> tableMetadataOpt = keyspaceMetadata.getTable(table);
                if (!tableMetadataOpt.isPresent()) {
                    throw new IllegalArgumentException("Table not found: " + table);
                }
                TableMetadata tableMetadata = tableMetadataOpt.get();

                Optional<ColumnMetadata> columnMetatdataOpt = tableMetadata.getColumn(columnName);
                if (!columnMetatdataOpt.isPresent()) {
                    throw new IllegalArgumentException("Column not found: " + columnName);
                }

                ColumnMetadata columnMetadata = columnMetatdataOpt.get();
                DataType columnType = columnMetadata.getType();
                if (columnType instanceof UserDefinedType) {
                    udtList.add((UserDefinedType) columnType);
                }
                else if (columnType instanceof ListType) {
                    DataType type = ((ListType) columnType).getElementType();
                    if (type instanceof UserDefinedType) {
                        udtList.add((UserDefinedType) type);
                    }
                }
                else if (columnType instanceof SetType) {
                    DataType type = ((SetType) columnType).getElementType();
                    if (type instanceof UserDefinedType) {
                        udtList.add((UserDefinedType) type);
                    }
                }
                else if (columnType instanceof MapType) {
                    DataType keyType = ((MapType) columnType).getKeyType();
                    DataType valueType = ((MapType) columnType).getValueType();
                    if (keyType instanceof UserDefinedType) {
                        udtList.add((UserDefinedType) keyType);
                    }
                    else if (valueType instanceof UserDefinedType) {
                        udtList.add((UserDefinedType) valueType);
                    }
                }
                else {
                    throw new IllegalArgumentException("Unhandled DataType for column " + columnName + ": " + columnType.getClass().getName()+"; MigrateDataType indicates this contains a UDT: "+ dataType);
                }
            }
            else {
                udtList.add(null);
            }
        }

        return udtList;
    }

    private List<Tuple2<UserDefinedType,UserDefinedType>> getFromToList(boolean isOrigin, PropertyHelper propertyHelper) {
        List<Integer> fromToIndexes = isOrigin ? ColumnsKeysTypes.getOriginToTargetColumnIndexes(propertyHelper) : ColumnsKeysTypes.getTargetToOriginColumnIndexes(propertyHelper);
        List<UserDefinedType> fromUDTList = isOrigin ? originUDTList : targetUDTList;
        List<UserDefinedType> toUDTList = isOrigin ? targetUDTList : originUDTList;

        List<Tuple2<UserDefinedType,UserDefinedType>> fromToList = new ArrayList<>();
        for (int i = 0; i < fromToIndexes.size(); i++) {
            int fromIndex = i;
            int toIndex = fromToIndexes.get(i);

            if (fromIndex >= 0 && toIndex >= 0) {
                UserDefinedType fromUDT = fromUDTList.get(fromIndex);
                UserDefinedType toUDT = toUDTList.get(toIndex);

                if (fromUDT == null || toUDT == null) {
                    fromToList.add(null);
                }
                else if (fromUDT.getKeyspace().equals(toUDT.getKeyspace()) &&
                        fromUDT.getName().equals(toUDT.getName())) {
                    // If these have same keyspace and name, we do not need to map them
                    fromToList.add(null);
                }
                else {
                    fromToList.add(new Tuple2<>(fromUDT, toUDT));
                }
            }
            else {
                fromToList.add(null);
            }
        }
        return fromToList;
    }

}
