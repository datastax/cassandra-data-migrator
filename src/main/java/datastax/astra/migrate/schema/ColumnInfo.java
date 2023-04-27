package datastax.astra.migrate.schema;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import lombok.Data;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Data
public class ColumnInfo {
    private String colName;
    private TypeInfo typeInfo;
    private boolean isPartitionKey;
    private boolean isClusteringKey;
    private boolean isNonKey;
    private boolean isCollection;
    private boolean isUDT;
    private boolean isFrozen;

    ColumnInfo(ColumnMetadata cm, boolean isPartitionKey, boolean isClusteringKey) {
        this.colName = cm.getName().toString();
        typeInfo = new TypeInfo(cm.getType());
        this.isPartitionKey = isPartitionKey;
        this.isClusteringKey = isClusteringKey;
        isNonKey = !isPartitionKey && !isClusteringKey;
        if (typeInfo.getTypeClass().equals(List.class) || typeInfo.getTypeClass().equals(Set.class)
                || typeInfo.getTypeClass().equals(Map.class)) {
            isCollection = true;
        }
        if (typeInfo.getTypeClass().equals(UdtValue.class)) {
            isUDT = true;
        }
        if (cm.getType().toString().toLowerCase(Locale.ROOT).contains(", frozen")) {
            isFrozen = true;
        }
    }
}
