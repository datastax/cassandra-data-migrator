package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class MigrateDataType {
    Class typeClass = Object.class;
    String dataTypeString = "";
    int type = -1;
    List<Class> subTypes = new ArrayList<Class>();
    private boolean isValid = false;
    private static int minType = 0;
    private static int maxType = 19;

    private static final List<Class> COLLECTION_TYPES = Arrays.asList(List.class, Set.class, Map.class);

    public MigrateDataType(String dataType) {
        dataTypeString = dataType;
        if (dataType.contains("%")) {
            int count = 1;
            for (String type : dataType.split("%")) {
                int typeAsInt = Integer.parseInt(type);
                if (count == 1) {
                    this.type = typeAsInt;
                } else {
                    subTypes.add(getType(typeAsInt));
                }
                count++;
            }
        } else {
            this.type = Integer.parseInt(dataType);
        }
        this.typeClass = getType(this.type);

        if (this.type >= minType && this.type <= maxType) {
            isValid = true;
            for (Object o : subTypes) {
                if (null == o || Object.class == o) {
                    isValid = false;
                }
            }
        }
        else {
            isValid = false;
        }
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

    private Class getType(int type) {
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
            case 16:
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

    public boolean isCollection() {
        return COLLECTION_TYPES.contains(typeClass);
    }

    public Class getType() {
        return this.typeClass;
    }

    public List<Class> getSubTypes() {
        return this.subTypes;
    }

    public boolean isValid() {
        return isValid;
    }

    public boolean parse(String s) {
        try {
            switch (this.type) {
                case 0:
                    return true;
                case 1:
                    Integer.parseInt(s);
                    return true;
                case 2:
                    Long.parseLong(s);
                    return true;
                case 3:
                    Double.parseDouble(s);
                    return true;
                case 4:
                    Instant.parse(s);
                    return true;
                case 9:
                    UUID.fromString(s);
                    return true;
                case 10:
                    Boolean.parseBoolean(s);
                    return true;
                case 12:
                    Float.parseFloat(s);
                    return true;
                case 13:
                    Byte.parseByte(s);
                    return true;
                case 14:
                    BigDecimal.valueOf(Double.parseDouble(s));
                    return true;
                case 15:
                    LocalDate.parse(s);
                    return true;
                case 17:
                    BigInteger.valueOf(Long.parseLong(s));
                    return true;
                case 18:
                    LocalTime.parse(s);
                    return true;
                case 19:
                    Short.parseShort(s);
                    return true;
                case 5: // Map
                case 6: // List
                case 7: // ByteBuffer
                case 8: // Set
                case 11: // TupleValue
                case 16: // UDT
                default:
                    throw new IllegalArgumentException("MigrateDataType " + this.type + "(" + this.typeClass.getName() + ") cannot currently parse this type");
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrateDataType that = (MigrateDataType) o;
        return type == that.type &&
                Objects.equals(subTypes, that.subTypes);
    }

    @Override
    public String toString() {
        return dataTypeString;
    }
}
