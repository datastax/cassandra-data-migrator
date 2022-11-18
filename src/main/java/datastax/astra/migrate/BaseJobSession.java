package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseJobSession {

    protected PreparedStatement sourceSelectStatement;
    protected PreparedStatement astraSelectStatement;
    protected PreparedStatement astraInsertStatement;

    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec,
    // reads/sec
    // then do the following to set the values as they are only applicable per JVM
    // (hence spark Executor)...
    // Rate = Total Throughput (write/read per sec) / Total Executors
    protected RateLimiter readLimiter;
    protected RateLimiter writeLimiter;
    protected Integer maxRetries = 10;

    protected CqlSession sourceSession;
    protected CqlSession astraSession;
    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();
    protected List<MigrateDataType> idColTypes = new ArrayList<MigrateDataType>();
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();

    protected Integer batchSize = 1;
    protected Integer printStatsAfter = 100000;

    protected Boolean writeTimeStampFilter = Boolean.FALSE;
    protected Long minWriteTimeStampFilter = 0l;
    protected Long maxWriteTimeStampFilter = Long.MAX_VALUE;
    protected Long customWritetime = 0l;

    protected List<Integer> writeTimeStampCols = new ArrayList<Integer>();
    protected List<Integer> ttlCols = new ArrayList<Integer>();
    protected Boolean isCounterTable;

    protected String sourceKeyspaceTable;
    protected String astraKeyspaceTable;

    protected Boolean hasRandomPartitioner;

    public List<MigrateDataType> getTypes(String types) {
        List<MigrateDataType> dataTypes = new ArrayList<MigrateDataType>();
        for (String type : types.split(",")) {
            dataTypes.add(new MigrateDataType(type));
        }

        return dataTypes;
    }

    public Object getData(MigrateDataType dataType, int index, Row sourceRow) {
        if (dataType.typeClass == Map.class) {
            return sourceRow.getMap(index, dataType.subTypes.get(0), dataType.subTypes.get(1));
        } else if (dataType.typeClass == List.class) {
            return sourceRow.getList(index, dataType.subTypes.get(0));
        } else if (dataType.typeClass == Set.class) {
            return sourceRow.getSet(index, dataType.subTypes.get(0));
        } else if (isCounterTable && dataType.typeClass == Long.class) {
            Object data = sourceRow.get(index, dataType.typeClass);
            if (data == null) {
                return new Long(0);
            }
        }

        return sourceRow.get(index, dataType.typeClass);
    }
}
