package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractJobSession {

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
    protected final RateLimiter readLimiter;
    protected final RateLimiter writeLimiter;
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

    protected List<Integer> writeTimeStampCols = new ArrayList<Integer>();
    protected List<Integer> ttlCols = new ArrayList<Integer>();
    protected Boolean isCounterTable;

    protected String sourceKeyspaceTable;
    protected String astraKeyspaceTable;

    protected Boolean hasRandomPartitioner;

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        batchSize = new Integer(sparkConf.get("spark.batchSize", "1"));
        printStatsAfter = new Integer(sparkConf.get("spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.readRateLimit", "20000")));
        writeLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.writeRateLimit", "40000")));
        maxRetries = Integer.parseInt(sparkConf.get("spark.maxRetries", "10"));

        sourceKeyspaceTable = sparkConf.get("spark.source.keyspaceTable");
        astraKeyspaceTable = sparkConf.get("spark.destination.keyspaceTable");

        isPreserveTTLWritetime = Boolean.parseBoolean(sparkConf.get("spark.preserveTTLWriteTime", "false"));
        if (isPreserveTTLWritetime) {
            String ttlColsStr = sparkConf.get("spark.source.ttl.cols");
            if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
                for (String ttlCol : ttlColsStr.split(",")) {
                    ttlCols.add(Integer.parseInt(ttlCol));
                }
            }
        }

        writeTimeStampFilter = Boolean
                .parseBoolean(sparkConf.get("spark.source.writeTimeStampFilter", "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
            String writeTimestampColsStr = sparkConf.get("spark.source.writeTimeStampFilter.cols");
            if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
                for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                    writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
                }
            }
        }

        String minWriteTimeStampFilterStr =
                sparkConf.get("spark.source.minWriteTimeStampFilter", "0");
        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr =
                sparkConf.get("spark.source.maxWriteTimeStampFilter", "0");
        if (null != maxWriteTimeStampFilterStr && maxWriteTimeStampFilterStr.trim().length() > 1) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        logger.info(" DEFAULT -- Write Batch Size: " + batchSize);
        logger.info(" DEFAULT -- Source Keyspace Table: " + sourceKeyspaceTable);
        logger.info(" DEFAULT -- Destination Keyspace Table: " + astraKeyspaceTable);
        logger.info(" DEFAULT -- ReadRateLimit: " + readLimiter.getRate());
        logger.info(" DEFAULT -- WriteRateLimit: " + writeLimiter.getRate());
        logger.info(" DEFAULT -- WriteTimestampFilter: " + writeTimeStampFilter);
        logger.info(" DEFAULT -- WriteTimestampFilterCols: " + writeTimeStampCols);
        logger.info(" DEFAULT -- isPreserveTTLWritetime: " + isPreserveTTLWritetime);
        logger.info(" DEFAULT -- TTLCols: " + ttlCols);

        hasRandomPartitioner = Boolean.parseBoolean(sparkConf.get("spark.source.hasRandomPartitioner", "false"));

        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.counterTable", "false"));
        selectColTypes = getTypes(sparkConf.get("spark.diff.select.types"));
        String partionKey = sparkConf.get("spark.query.cols.partitionKey");
        String idCols = sparkConf.get("spark.query.cols.id");
        idColTypes = getTypes(sparkConf.get("spark.query.cols.id.types"));

        String selectCols = sparkConf.get("spark.query.cols.select");

        String idBinds = "";
        int count = 1;
        for (String str : idCols.split(",")) {
            if (count > 1) {
                idBinds = idBinds + " and " + str + "= ?";
            } else {
                idBinds = str + "= ?";
            }
            count++;
        }

        sourceSelectCondition = sparkConf.get("spark.query.cols.select.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");

        astraSelectStatement = astraSession.prepare(
                "select " + selectCols + " from " + astraKeyspaceTable
                        + " where " + idBinds);
    }

    public List<MigrateDataType> getTypes(String types) {
        List<MigrateDataType> dataTypes = new ArrayList<MigrateDataType>();
        for (String type : types.split(",")) {
            dataTypes.add(new MigrateDataType(type));
        }

        return dataTypes;
    }

    public int getLargestTTL(Row sourceRow) {
        int ttl = 0;
        for (Integer ttlCol : ttlCols) {
            ttl = Math.max(ttl, sourceRow.getInt(ttlCol));
        }
        return ttl;
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        long writeTimestamp = 0;
        for (Integer writeTimeStampCol : writeTimeStampCols) {
            writeTimestamp = Math.max(writeTimestamp, sourceRow.getLong(writeTimeStampCol));
        }
        return writeTimestamp;
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow) {
        BoundStatement boundSelectStatement = selectStatement.bind();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            boundSelectStatement = boundSelectStatement.set(index, getData(dataType, index, sourceRow),
                    dataType.typeClass);
        }

        return boundSelectStatement;
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
