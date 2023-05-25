package datastax.astra.migrate.schema;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.*;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

@Data
public class TypeInfo {
    private static Map<DataType, Class> cqlToJavaTypeMap = loadCqlToJavaTypeMap();
    private Class typeClass = Object.class;
    private List<Class> subTypes = new ArrayList<Class>();
    private boolean isCounter = false;
    private boolean isFrozen = false;

    public TypeInfo(DataType dataType) {
        typeClass = getDataTypeToType(dataType);

        if (dataType instanceof UserDefinedType) {
            isFrozen = ((UserDefinedType) dataType).isFrozen();
        } else if (dataType instanceof ListType) {
            subTypes.add(getDataTypeToType(((ListType) dataType).getElementType()));
            isFrozen = ((ListType) dataType).isFrozen();
        } else if (dataType instanceof SetType) {
            subTypes.add(getDataTypeToType(((SetType) dataType).getElementType()));
            isFrozen = ((SetType) dataType).isFrozen();
        } else if (dataType instanceof MapType) {
            subTypes.add(getDataTypeToType(((MapType) dataType).getKeyType()));
            subTypes.add(getDataTypeToType(((MapType) dataType).getValueType()));
            isFrozen = ((MapType) dataType).isFrozen();
        } else if (DataTypes.COUNTER.equals(dataType)) {
            isCounter = true;
        }
    }

    private static Map loadCqlToJavaTypeMap() {
        Map<DataType, Class> typeMap = new HashMap<>();
        typeMap.put(DataTypes.ASCII, String.class);
        typeMap.put(DataTypes.BIGINT, Long.class);
        typeMap.put(DataTypes.BLOB, ByteBuffer.class);
        typeMap.put(DataTypes.BOOLEAN, Boolean.class);
        typeMap.put(DataTypes.COUNTER, Long.class);
        typeMap.put(DataTypes.DATE, LocalDate.class);
        typeMap.put(DataTypes.DECIMAL, BigDecimal.class);
        typeMap.put(DataTypes.DOUBLE, Double.class);
        typeMap.put(DataTypes.FLOAT, Float.class);
        typeMap.put(DataTypes.INT, Integer.class);
        typeMap.put(DataTypes.INET, String.class);
        typeMap.put(DataTypes.SMALLINT, Short.class);
        typeMap.put(DataTypes.TEXT, String.class);
        typeMap.put(DataTypes.TIME, LocalTime.class);
        typeMap.put(DataTypes.TIMESTAMP, Instant.class);
        typeMap.put(DataTypes.TIMEUUID, UUID.class);
        typeMap.put(DataTypes.TINYINT, Byte.class);
        typeMap.put(DataTypes.UUID, UUID.class);
        typeMap.put(DataTypes.VARINT, BigInteger.class);

        return typeMap;
    }

    private Class getDataTypeToType(DataType dataType) {
        if (dataType instanceof UserDefinedType) return UdtValue.class;
        else if (dataType instanceof ListType) {
            return List.class;
        } else if (dataType instanceof SetType) {
            return Set.class;
        } else if (dataType instanceof MapType) {
            return Map.class;
        } else if (dataType instanceof TupleType) {
            return TupleValue.class;
        }

        return cqlToJavaTypeMap.get(dataType);
    }

    public String toString() {
        return "Type: " + typeClass.toString() + " SubTypes: " + subTypes.toString();
    }

    public boolean diff(Object source, Object astra) {
        if (source == null && astra == null) {
            return false;
        } else if (source == null && astra != null) {
            return true;
        } else if (source != null && astra == null) {
            return true;
        }

        return !source.equals(astra);
    }
}
