package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        batchSize = new Integer(Util.getSparkPropOr(sc, "spark.batchSize", "1"));
        printStatsAfter = new Integer(Util.getSparkPropOr(sc, "spark.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, "spark.readRateLimit", "20000")));
        writeLimiter = RateLimiter.create(new Integer(Util.getSparkPropOr(sc, "spark.writeRateLimit", "40000")));
        maxRetries = Integer.parseInt(sc.get("spark.maxRetries", "10"));

        sourceKeyspaceTable = Util.getSparkProp(sc, "spark.origin.keyspaceTable");
        astraKeyspaceTable = Util.getSparkProp(sc, "spark.target.keyspaceTable");

        String ttlColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.ttl.cols");
        if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
            for (String ttlCol : ttlColsStr.split(",")) {
                ttlCols.add(Integer.parseInt(ttlCol));
            }
        }

        String writeTimestampColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.writetime.cols");
        if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
            for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
            }
        }

        writeTimeStampFilter = Boolean
                .parseBoolean(Util.getSparkPropOr(sc, "spark.origin.writeTimeStampFilter", "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
        }

        String minWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, "spark.origin.minWriteTimeStampFilter", "0");
        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr =
                Util.getSparkPropOr(sc, "spark.origin.maxWriteTimeStampFilter", "0");
        if (null != maxWriteTimeStampFilterStr && maxWriteTimeStampFilterStr.trim().length() > 1) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        String customWriteTimeStr =
                Util.getSparkPropOr(sc, "spark.target.custom.writeTime", "0");
        if (null != customWriteTimeStr && customWriteTimeStr.trim().length() > 1 && StringUtils.isNumeric(customWriteTimeStr.trim())) {
            customWritetime = Long.parseLong(customWriteTimeStr);
        }

        logger.info("PARAM -- Write Batch Size: " + batchSize);
        logger.info("PARAM -- Source Keyspace Table: " + sourceKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: " + astraKeyspaceTable);
        logger.info("PARAM -- ReadRateLimit: " + readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: " + writeLimiter.getRate());
        logger.info("PARAM -- TTLCols: " + ttlCols);
        logger.info("PARAM -- WriteTimestampFilterCols: " + writeTimeStampCols);
        logger.info("PARAM -- WriteTimestampFilter: " + writeTimeStampFilter);

        String selectCols = Util.getSparkProp(sc, "spark.query.origin");
        String partionKey = Util.getSparkProp(sc, "spark.query.origin.partitionKey");
        String sourceSelectCondition = Util.getSparkPropOrEmpty(sc, "spark.query.condition");

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        String[] allCols = selectCols.split(",");
        ttlCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",ttl(" + allCols[col] + ")");
        });
        writeTimeStampCols.forEach(col -> {
            selectTTLWriteTimeCols.append(",writetime(" + allCols[col] + ")");
        });
        String fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING";
        sourceSelectStatement = sourceSession.prepare(fullSelectQuery);
        logger.info("PARAM -- Query used: " + fullSelectQuery);

        selectColTypes = getTypes(Util.getSparkProp(sc, "spark.query.types"));
        String idCols = Util.getSparkPropOrEmpty(sc, "spark.query.target.id");
        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String insertCols = Util.getSparkPropOrEmpty(sc, "spark.query.target");
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

        hasRandomPartitioner = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.origin.hasRandomPartitioner", "false"));
        isCounterTable = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.counterTable", "false"));
        if (isCounterTable) {
            String updateSelectMappingStr = Util.getSparkPropOr(sc, "spark.counterTable.cql.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = Util.getSparkProp(sc, "spark.counterTable.cql");
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

            String fullInsertQuery = "insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")";
            if (!ttlCols.isEmpty()) {
                fullInsertQuery += " USING TTL ?";
                if (!writeTimeStampCols.isEmpty()) {
                    fullInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (!writeTimeStampCols.isEmpty()) {
                fullInsertQuery += " USING TIMESTAMP ?";
            }
            astraInsertStatement = astraSession.prepare(fullInsertQuery);
        }
    }

    public int getLargestTTL(Row sourceRow) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> sourceRow.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> sourceRow.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
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

}
