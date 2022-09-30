package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractJobSession extends BaseJobSession {



    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        batchSize = new Integer(sparkConf.get("spark.migrate.batchSize", "1"));
        printStatsAfter = new Integer(sparkConf.get("spark.migrate.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.readRateLimit", "20000")));
        writeLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.writeRateLimit", "40000")));
        maxRetries = Integer.parseInt(sparkConf.get("spark.migrate.maxRetries", "10"));

        sourceKeyspaceTable = sparkConf.get("spark.migrate.source.keyspaceTable");
        astraKeyspaceTable = sparkConf.get("spark.migrate.destination.keyspaceTable");
        isAstraDestination = Boolean.parseBoolean(sparkConf.get("spark.migrate.destination.isAstra", "true"));
        isPreserveTTLWritetime = Boolean.parseBoolean(sparkConf.get("spark.migrate.preserveTTLWriteTime", "false"));
        if (isPreserveTTLWritetime) {
            String ttlColsStr = sparkConf.get("spark.migrate.source.ttl.cols");
            if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
                for (String ttlCol : ttlColsStr.split(",")) {
                    ttlCols.add(Integer.parseInt(ttlCol));
                }
            }
        }

        writeTimeStampFilter = Boolean
                .parseBoolean(sparkConf.get("spark.migrate.source.writeTimeStampFilter", "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
            String writeTimestampColsStr = sparkConf.get("spark.migrate.source.writeTimeStampFilter.cols");
            if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
                for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                    writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
                }
            }
        }

        String minWriteTimeStampFilterStr =
                sparkConf.get("spark.migrate.source.minWriteTimeStampFilter", "0");
        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr =
                sparkConf.get("spark.migrate.source.maxWriteTimeStampFilter", "0");
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

        hasRandomPartitioner = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.hasRandomPartitioner", "false"));
        trimColumnRow = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.trimColumnRow", "false"));
        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.counterTable", "false"));

        counterDeltaMaxIndex = Integer
                .parseInt(sparkConf.get("spark.migrate.source.counterTable.update.max.counter.index", "0"));

        String partionKey = sparkConf.get("spark.migrate.query.cols.partitionKey");
        String idCols = sparkConf.get("spark.migrate.query.cols.id");
        idColTypes = getTypes(sparkConf.get("spark.migrate.query.cols.id.types"));

        String selectCols = sparkConf.get("spark.migrate.query.cols.select");

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

        sourceSelectCondition = sparkConf.get("spark.migrate.query.cols.select.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");

        astraSelectStatement = astraSession.prepare(
                "select " + selectCols + " from " + astraKeyspaceTable
                        + " where " + idBinds);
    }



}
