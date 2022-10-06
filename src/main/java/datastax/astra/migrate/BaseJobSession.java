package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract  class BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected PreparedStatement sourceSelectStatement;
    protected String sourceSelectCondition;

    protected PreparedStatement astraSelectStatement;

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

    protected Integer batchSize = 1;
    protected Integer printStatsAfter = 100000;

    protected Boolean isPreserveTTLWritetime = Boolean.FALSE;
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

}
