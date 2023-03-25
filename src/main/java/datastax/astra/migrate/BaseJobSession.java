package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.properties.KnownProperties;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseJobSession {

    protected PreparedStatement originSelectStatement;
    protected PreparedStatement targetSelectStatement;
    protected PreparedStatement targetInsertStatement;
    protected ConsistencyLevel readConsistencyLevel;
    protected ConsistencyLevel writeConsistencyLevel;

    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec,
    // reads/sec
    // then do the following to set the values as they are only applicable per JVM
    // (hence spark Executor)...
    // Rate = Total Throughput (write/read per sec) / Total Executors
    protected RateLimiter readLimiter;
    protected RateLimiter writeLimiter;
    protected Integer maxRetries = 10;

    protected CqlSession originSessionSession;
    protected CqlSession targetSession;
    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();
    protected List<MigrateDataType> idColTypes = new ArrayList<MigrateDataType>();
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();

    protected Integer batchSize = 1;
    protected Integer fetchSizeInRows = 1000;
    protected Integer printStatsAfter = 100000;

    protected Boolean writeTimeStampFilter = Boolean.FALSE;
    protected Long minWriteTimeStampFilter = 0l;
    protected Long maxWriteTimeStampFilter = Long.MAX_VALUE;
    protected Long customWritetime = 0l;

    protected List<Integer> writeTimeStampCols = new ArrayList<Integer>();
    protected List<Integer> ttlCols = new ArrayList<Integer>();
    protected Boolean isCounterTable;

    protected String originKeyspaceTable;
    protected String targetKeyspaceTable;

    protected Boolean hasRandomPartitioner;
    protected Boolean filterData;
    protected String filterColName;
    protected String filterColType;
    protected Integer filterColIndex;
    protected String filterColValue;

    protected String[] allCols;
    protected String tsReplaceValStr;
    protected long tsReplaceVal;

    protected BaseJobSession(SparkConf sc) {
        readConsistencyLevel = Util.mapToConsistencyLevel(Util.getSparkPropOrEmpty(sc, KnownProperties.READ_CL));
        writeConsistencyLevel = Util.mapToConsistencyLevel(Util.getSparkPropOrEmpty(sc, KnownProperties.WRITE_CL));
    }

    public String getKey(Row originRow) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, originRow));
            } else {
                key.append(" %% " + getData(dataType, index, originRow));
            }
        }

        return key.toString();
    }

    public List<MigrateDataType> getTypes(String types) {
        List<MigrateDataType> dataTypes = new ArrayList<MigrateDataType>();
        for (String type : types.split(",")) {
            dataTypes.add(new MigrateDataType(type));
        }

        return dataTypes;
    }

    public Object getData(MigrateDataType dataType, int index, Row originRow) {
        if (dataType.typeClass == Map.class) {
            return originRow.getMap(index, dataType.subTypes.get(0), dataType.subTypes.get(1));
        } else if (dataType.typeClass == List.class) {
            return originRow.getList(index, dataType.subTypes.get(0));
        } else if (dataType.typeClass == Set.class) {
            return originRow.getSet(index, dataType.subTypes.get(0));
        } else if (isCounterTable && dataType.typeClass == Long.class) {
            Object data = originRow.get(index, dataType.typeClass);
            if (data == null) {
                return new Long(0);
            }
        }

        return originRow.get(index, dataType.typeClass);
    }
}
