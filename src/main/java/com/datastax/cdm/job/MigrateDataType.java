package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class MigrateDataType {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    Class typeClass = Object.class;
    String dataTypeString = "";
    int type = -1;
    List<Class> subTypeClasses = new ArrayList<Class>();
    List<MigrateDataType> subTypeTypes = new ArrayList<MigrateDataType>();
    private boolean isValid = false;
    private static int minType = 0;
    private static int maxType = 19;
    public static final int UNKNOWN_TYPE = 99;
    public static final int UDT_TYPE = 16;
    private boolean hasUDT = false;

    public MigrateDataType(String dataType) {
        dataTypeString = dataType;
        if (dataType.contains("%")) {
            int count = 1;
            for (String type : dataType.split("%")) {
                int typeAsInt = typeAsInt(type);
                if (typeAsInt == UDT_TYPE) this.hasUDT = true;
                if (count == 1) {
                    this.type = typeAsInt;
                } else {
                    subTypeClasses.add(getTypeClass(typeAsInt));
                    subTypeTypes.add(new MigrateDataType(type));
                }
                count++;
            }
        } else {
            this.type = typeAsInt(dataType);
            this.hasUDT = this.type==UDT_TYPE;
        }

        this.typeClass = getTypeClass(this.type);

        if ((this.type >= minType && this.type <= maxType) || this.type == UNKNOWN_TYPE) {
            isValid = true;
            for (Object o : subTypeClasses) {
                if (null == o || Object.class == o) {
                    isValid = false;
                }
            }
        }
        else {
            isValid = false;
        }
    }

    public MigrateDataType() {
        this.dataTypeString = "UNKNOWN";
        this.type = UNKNOWN_TYPE;
        this.typeClass = getTypeClass(this.type);
        isValid = true;
    }

    private int typeAsInt(String dataType) {
        int rtn = -1;
        try {
            rtn = Integer.parseInt(dataType);
        } catch (NumberFormatException e) {
            rtn = -1;
        }
        return rtn;
    }

    public boolean diff(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) {
            return false;
        } else if (obj1 == null && obj2 != null) {
            logger.info("DEBUG: obj1 is null and obj2 is not null");
            return true;
        } else if (obj1 != null && obj2 == null) {
            logger.info("DEBUG: obj2 is null and obj1 is not null");
            return true;
        }

        return !obj1.equals(obj2);
    }

    @SuppressWarnings("unchecked")
    public static Object convert(Object value, MigrateDataType fromDataType, MigrateDataType toDataType, CodecRegistry codecRegistry) {
        if (null==value) return null;
        if (null==fromDataType || null==toDataType || null==codecRegistry)
            throw new IllegalArgumentException("fromDataType, toDataType, and codecRegistry must not be null");
        Class<?> fromClass = fromDataType.getTypeClass();
        Class<?> toClass = toDataType.getTypeClass();
        DataType cqlType = toDataType.getCqlDataType();

        if (!value.getClass().equals(fromClass)) {
            throw new IllegalArgumentException("Value is not of type " + fromClass.getName() + " but of type " + value.getClass().getName());
        }

        TypeCodec<Object> fromCodec = (TypeCodec<Object>) codecRegistry.codecFor(cqlType, fromClass);
        if (fromCodec == null) {
            throw new IllegalArgumentException("No codec found in codecRegistry for Java type " + fromClass.getName() + " to CQL type " + toDataType);
        }
        TypeCodec<Object> toCodec = (TypeCodec<Object>) codecRegistry.codecFor(cqlType, toClass);
        if (toCodec == null) {
            throw new IllegalArgumentException("No codec found in codecRegistry for Java type " + toClass.getName() + " to CQL type " + toDataType);
        }
        ByteBuffer encoded = fromCodec.encode(value, ProtocolVersion.DEFAULT);
        return toCodec.decode(encoded, ProtocolVersion.DEFAULT);
    }

    private Class getTypeClass(int type) {
        switch (type) {
            case 0:
                return String.class;
            case 1:
                return Integer.class;
            case 2:
                return Long.class;
            case 3:
                return Double.class;
            case 4:
                return Instant.class;
            case 5:
                return Map.class;
            case 6:
                return List.class;
            case 7:
                return ByteBuffer.class;
            case 8:
                return Set.class;
            case 9:
                return UUID.class;
            case 10:
                return Boolean.class;
            case 11:
                return TupleValue.class;
            case 12:
                return Float.class;
            case 13:
                return Byte.class;
            case 14:
                return BigDecimal.class;
            case 15:
                return LocalDate.class;
            case UDT_TYPE:
                return UdtValue.class;
            case 17:
                return BigInteger.class;
            case 18:
                return LocalTime.class;
            case 19:
                return Short.class;
        }

        return Object.class;
    }

    private DataType getCqlDataType(int type) {
        switch (type) {
            case 0: return DataTypes.TEXT;
            case 1: return DataTypes.INT;
            case 2: return DataTypes.BIGINT;
            case 3: return DataTypes.DOUBLE;
            case 4: return DataTypes.TIMESTAMP;
            case 5: return null;
            case 6: return null;
            case 7: return DataTypes.BLOB;
            case 8: return null;
            case 9: return DataTypes.UUID;
            case 10: return DataTypes.BOOLEAN;
            case 11: return null;
            case 12: return DataTypes.FLOAT;
            case 13: return DataTypes.TINYINT;
            case 14: return DataTypes.DECIMAL;
            case 15: return DataTypes.DATE;
            case 16: return null;
            case 17: return DataTypes.VARINT;
            case 18: return DataTypes.TIME;
            case 19: return DataTypes.SMALLINT;
            default: return null;
        }
    }

    public DataType getCqlDataType() {
        DataType dt = getCqlDataType(this.type);
        if (null != dt) return dt;
        switch (this.type) {
            case 5:
                DataType keyType = getCqlDataType(this.subTypeTypes.get(0).type);
                DataType valueType = getCqlDataType(this.subTypeTypes.get(1).type);
                if (null != keyType && null != valueType)
                    return DataTypes.mapOf(keyType, valueType);
                break;
            case 6:
                DataType listType = getCqlDataType(this.subTypeTypes.get(0).type);
                if (null != listType)
                    return DataTypes.listOf(listType);
                break;
            case 8:
                DataType setType = getCqlDataType(this.subTypeTypes.get(0).type);
                if (null != setType)
                    return DataTypes.setOf(setType);
                break;
            case 11:
                DataType val1Type = getCqlDataType(this.subTypeTypes.get(0).type);
                DataType val2Type = getCqlDataType(this.subTypeTypes.get(1).type);
                if (null != val1Type && null != val2Type)
                    return DataTypes.tupleOf(val1Type, val2Type);
                break;
            case 16:
                return null;  // TODO need to implement UDT properly...
            default:
                return null;
        }
        return null;
    }

    public Class getTypeClass() {
        return this.typeClass;
    }

    public List<Class> getSubTypeClasses() {
        return this.subTypeClasses;
    }

    public List<MigrateDataType> getSubTypeTypes() {return this.subTypeTypes;}

    public boolean isValid() {
        return isValid;
    }

    public boolean hasUDT() { return hasUDT; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrateDataType that = (MigrateDataType) o;
        return type == that.type &&
                Objects.equals(subTypeClasses, that.subTypeClasses);
    }

    @Override
    public String toString() {
        return dataTypeString;
    }
}
