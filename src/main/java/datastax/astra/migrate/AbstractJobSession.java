package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractJobSession extends BaseJobSession {


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

        logger.info("PARAM -- Write Batch Size: " + batchSize);
        logger.info("PARAM -- Source Keyspace Table: " + sourceKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: " + astraKeyspaceTable);
        logger.info("PARAM -- ReadRateLimit: " + readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: " + writeLimiter.getRate());
        logger.info("PARAM -- WriteTimestampFilter: " + writeTimeStampFilter);
        logger.info("PARAM -- WriteTimestampFilterCols: " + writeTimeStampCols);
        logger.info("PARAM -- isPreserveTTLWritetime: " + isPreserveTTLWritetime);
        logger.info("PARAM -- isPreserveTTLWritetime: " + isPreserveTTLWritetime);
        logger.info("PARAM -- TTLCols: " + ttlCols);

        String selectCols = sparkConf.get("spark.query.source");
        String partionKey = sparkConf.get("spark.query.source.partitionKey");
        selectColTypes = getTypes(sparkConf.get("spark.query.types"));
        String idCols = sparkConf.get("spark.query.destination.id", "");
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);
        sourceSelectCondition = sparkConf.get("spark.query.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");

        String insertCols = sparkConf.get("spark.query.destination", "");
        if (null == insertCols || insertCols.trim().isEmpty()) {
            insertCols = selectCols;
        }
        String insertBinds = "";
        for (String str : idCols.split(",")) {
            if (insertBinds.isEmpty()) {
                insertBinds = str + "= ?";
            } else {
                insertBinds += " and " + str + "= ?";
            }
        }
        astraSelectStatement = astraSession.prepare(
                "select " + insertCols + " from " + astraKeyspaceTable
                        + " where " + insertBinds);

        hasRandomPartitioner = Boolean.parseBoolean(sparkConf.get("spark.source.hasRandomPartitioner", "false"));
        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.counterTable", "false"));
        if (isCounterTable) {
            String updateSelectMappingStr = sparkConf.get("spark.counterTable.cql.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = sparkConf.get("spark.counterTable.cql");
            astraInsertStatement = astraSession.prepare(counterTableUpdate);
        } else {
            insertBinds = "";
            for (String str : insertCols.split(",")) {
                if (insertBinds.isEmpty()) {
                    insertBinds += "?";
                } else {
                    insertBinds += ", ?";
                }
            }

            if (isPreserveTTLWritetime) {
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ") using TTL ? and TIMESTAMP ?");
            } else {
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")");
            }
        }
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
