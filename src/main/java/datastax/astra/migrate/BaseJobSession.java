package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.schema.TableInfo;
import datastax.astra.migrate.schema.TypeInfo;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseJobSession {

    protected PreparedStatement sourceSelectStatement;
    protected PreparedStatement astraSelectStatement;
    protected PreparedStatement astraInsertStatement;
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
    protected AtomicLong readCounter = new AtomicLong(0);

    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();

    protected Integer batchSize = 1;
    protected Integer fetchSizeInRows = 1000;
    protected Integer printStatsAfter = 100000;

    protected Boolean writeTimeStampFilter = Boolean.FALSE;
    protected Long minWriteTimeStampFilter = 0l;
    protected Long maxWriteTimeStampFilter = Long.MAX_VALUE;
    protected Long customWritetime = 0l;

    protected Boolean isCounterTable = false;

    protected String sourceKeyspaceTable;
    protected String astraKeyspaceTable;

    protected Boolean hasRandomPartitioner;
    protected Boolean filterData;
    protected String filterColName;
    protected String filterColType;
    protected Integer filterColIndex;
    protected String filterColValue;
    protected String sourceSelectCondition;

    protected BaseJobSession(SparkConf sc) {
        readConsistencyLevel = Util.mapToConsistencyLevel(Util.getSparkPropOrEmpty(sc, "spark.consistency.read"));
        writeConsistencyLevel = Util.mapToConsistencyLevel(Util.getSparkPropOrEmpty(sc, "spark.consistency.write"));
        readLimiter = RateLimiter.create(Integer.parseInt(Util.getSparkPropOr(sc, "spark.readRateLimit", "20000")));
        sourceKeyspaceTable = sc.get("spark.origin.keyspaceTable");
        hasRandomPartitioner = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.origin.hasRandomPartitioner", "false"));
        sourceSelectCondition = Util.getSparkPropOrEmpty(sc, "spark.query.condition");
        if (!sourceSelectCondition.isEmpty() && !sourceSelectCondition.trim().toUpperCase().startsWith("AND")) {
            sourceSelectCondition = " AND " + sourceSelectCondition;
        }

        printStatsAfter = Integer.parseInt(Util.getSparkPropOr(sc, "spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }
    }

    public Object getData(TypeInfo typeInfo, int index, Row row) {
        if (typeInfo.getTypeClass() == Map.class) {
            return row.getMap(index, typeInfo.getSubTypes().get(0), typeInfo.getSubTypes().get(1));
        } else if (typeInfo.getTypeClass() == List.class) {
            return row.getList(index, typeInfo.getSubTypes().get(0));
        } else if (typeInfo.getTypeClass() == Set.class) {
            return row.getSet(index, typeInfo.getSubTypes().get(0));
        } else if (isCounterTable && typeInfo.getTypeClass() == Long.class) {
            Object data = row.get(index, typeInfo.getTypeClass());
            if (data == null) {
                return Long.valueOf(0);
            }
        }

        return row.get(index, typeInfo.getTypeClass());
    }

    public int getFieldSize(TypeInfo typeInfo, int index, Row row) {
        return SerializationUtils.serialize((Serializable) getData(typeInfo, index, row)).length;
    }

    public String getKey(Row sourceRow, TableInfo tableInfo) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < tableInfo.getKeyColumns().size(); index++) {
            TypeInfo typeInfo = tableInfo.getIdColumns().get(index).getTypeInfo();
            if (index == 0) {
                key.append(getData(typeInfo, index, sourceRow));
            } else {
                key.append(" %% " + getData(typeInfo, index, sourceRow));
            }
        }

        return key.toString();
    }

}
