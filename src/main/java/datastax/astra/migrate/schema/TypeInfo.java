package datastax.astra.migrate.schema;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
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
    private static Map<String, Class> typeMap = loadTypeMap();
    private Class typeClass = Object.class;
    private List<Class> subTypes = new ArrayList<Class>();
    private boolean isCounter = false;

    public TypeInfo(DataType dataType) {
        this(dataType.toString());
    }

    public TypeInfo(String dataTypeStr) {
        int sIdx = dataTypeStr.indexOf('(');
        int eIdx = -1;
        if (sIdx != -1) {
            eIdx = dataTypeStr.substring(sIdx + 1).indexOf('(');
            if (eIdx == -1) {
                eIdx = dataTypeStr.substring(sIdx + 1).indexOf(',');
            }
            eIdx += sIdx + 1;
        }

        if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("list")) {
            typeClass = List.class;
            subTypes.add(typeMap.get(dataTypeStr.substring(sIdx + 1, eIdx).toLowerCase(Locale.ROOT)));
        } else if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("set")) {
            typeClass = Set.class;
            subTypes.add(typeMap.get(dataTypeStr.substring(sIdx + 1, eIdx).toLowerCase(Locale.ROOT)));
        } else if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("map")) {
            typeClass = Map.class;
            subTypes.add(typeMap.get(dataTypeStr.substring(sIdx + 1, dataTypeStr.indexOf("=>")).trim().toLowerCase(Locale.ROOT)));
            subTypes.add(typeMap.get(dataTypeStr.substring(dataTypeStr.indexOf("=>") + 2, dataTypeStr.indexOf(',')).trim().toLowerCase(Locale.ROOT)));
        } else if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("udt")) {
            typeClass = UdtValue.class;
        } else if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("tuple")) {
            typeClass = TupleValue.class;
        } else if (dataTypeStr.toLowerCase(Locale.ROOT).startsWith("counter")) {
            typeClass = Long.class;
            isCounter = true;
        } else {
            typeClass = typeMap.get(dataTypeStr.toLowerCase(Locale.ROOT));
        }
    }

    private static Map loadTypeMap() {
        Map typeMap = new HashMap<>();
        typeMap.put("ascii", String.class);
        typeMap.put("bigint", Long.class);
        typeMap.put("blob", ByteBuffer.class);
        typeMap.put("boolean", Boolean.class);
        typeMap.put("counter", Long.class);
        typeMap.put("date", LocalDate.class);
        typeMap.put("decimal", BigDecimal.class);
        typeMap.put("double", Double.class);
        typeMap.put("float", Float.class);
        typeMap.put("int", Integer.class);
        typeMap.put("inet", String.class);
        typeMap.put("smallint", Short.class);
        typeMap.put("text", String.class);
        typeMap.put("time", LocalTime.class);
        typeMap.put("timestamp", Instant.class);
        typeMap.put("timeuuid", UUID.class);
        typeMap.put("tinyint", Byte.class);
        typeMap.put("udt", UdtValue.class);
        typeMap.put("uuid", UUID.class);
        typeMap.put("varchar", String.class);
        typeMap.put("varint", BigInteger.class);

        return typeMap;
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
