package datastax.astra.migrate.schema;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import lombok.Data;
import lombok.ToString;

import java.util.*;
import java.util.stream.Collectors;

@Data
@ToString(onlyExplicitlyIncluded = true)
public class TableInfo {
    private static TableInfo tableInfo;

    @ToString.Include
    private List<ColumnInfo> columns = new ArrayList<>();

    private List<ColumnInfo> nonKeyColumns;
    private List<ColumnInfo> partitionColumns;
    private List<ColumnInfo> idColumns;
    private List<String> partitionKeyColumns;
    private List<String> keyColumns;
    private List<String> allColumns;
    private List<String> ttlAndWriteTimeColumns;
    private String desc;
    private boolean isCounterTable = false;

    protected TableInfo(CqlSession session, String keySpace, String table, String selectColsString) {
        List<String> selectCols = selectColsString.isEmpty() ? Collections.emptyList() :
                Arrays.asList(selectColsString.toLowerCase(Locale.ROOT).split(","));
        TableMetadata tm = session.getMetadata().getKeyspace(keySpace).get().getTable(table).get();
        desc = tm.describe(false);
        if (desc.toLowerCase(Locale.ROOT).contains("counter")) {
            isCounterTable = true;
        }

        partitionColumns = getPartitionKeyColumns(tm);
        partitionKeyColumns = colInfoToList(partitionColumns);

        idColumns = partitionColumns.stream().collect(Collectors.toList());
        idColumns.addAll(getClusteringKeyColumns(tm));
        keyColumns = colInfoToList(idColumns);

        nonKeyColumns = getNonKeyColumns(tm, keyColumns, selectCols);
        columns.addAll(idColumns);
        columns.addAll(nonKeyColumns);
        allColumns = colInfoToList(columns);

        ttlAndWriteTimeColumns = loadTtlAndWriteTimeCols();
    }

    public static TableInfo getInstance(CqlSession session, String keySpace, String table, String selectColsString) {
        if (tableInfo == null) {
            synchronized (TableInfo.class) {
                if (tableInfo == null) {
                    tableInfo = new TableInfo(session, keySpace, table, selectColsString);
                }
            }
        }

        return tableInfo;
    }

    private List<String> loadTtlAndWriteTimeCols() {
        return columns.stream()
                .filter(cm -> cm.isNonKey())
                .filter(cm -> ((!cm.isCollection() && !cm.isUDT()) || cm.isFrozen()))
                .map(cm -> cm.getColName())
                .collect(Collectors.toList());
    }

    private List<ColumnInfo> getPartitionKeyColumns(TableMetadata tm) {
        return tm.getPartitionKey().stream().map(
                cm -> (new ColumnInfo(cm, true, false))
        ).collect(Collectors.toList());
    }

    private List<ColumnInfo> getClusteringKeyColumns(TableMetadata tm) {
        return tm.getClusteringColumns().keySet().stream().map(cm ->
                new ColumnInfo(cm, false, true)
        ).collect(Collectors.toList());
    }

    private List<ColumnInfo> getNonKeyColumns(TableMetadata tm, List keyColumnsNames, List<String> selectCols) {
        List<ColumnInfo> otherCols = new ArrayList<>();
        tm.getColumns().values().stream().forEach(cm -> {
            if (!keyColumnsNames.contains(cm.getName().toString())) {
                if (selectCols.isEmpty() || selectCols.contains(cm.getName().toString().toLowerCase(Locale.ROOT))) {
                    ColumnInfo ci = new ColumnInfo(cm, false, false);
                    otherCols.add(ci);
                }
            }
        });

        return otherCols;
    }

    private List<String> colInfoToList(List<ColumnInfo> listColInfo) {
        return listColInfo.stream().map(ColumnInfo::getColName).collect(Collectors.toList());
    }
}
