package datastax.cdm.job;

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
    List<Class> subTypeClasses = new ArrayList<Class>();
    List<MigrateDataType> subTypeTypes = new ArrayList<MigrateDataType>();
    private boolean isValid = false;
    private static int minType = 0;
    private static int maxType = 19;
    public static final int UNKNOWN_TYPE = 99;

    public MigrateDataType(String dataType) {
        dataTypeString = dataType;
        if (dataType.contains("%")) {
            int count = 1;
            for (String type : dataType.split("%")) {
                int typeAsInt = typeAsInt(type);
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
            return true;
        } else if (obj1 != null && obj2 == null) {
            return true;
        }

        return !obj1.equals(obj2);
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
