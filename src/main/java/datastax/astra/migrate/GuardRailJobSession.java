package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class GuardRailJobSession extends BaseJobSession  {

    public static Logger logger = Logger.getLogger(GuardRailJobSession.class);
    private static GuardRailJobSession guardRailJobSession;
    protected AtomicLong readCounter = new AtomicLong(0);
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();

    public static GuardRailJobSession getInstance(CqlSession sourceSession, SparkConf sparkConf) {
        if (guardRailJobSession == null) {
            synchronized (GuardRailJobSession.class) {
                if (guardRailJobSession == null) {
                    guardRailJobSession = new GuardRailJobSession(sourceSession, sparkConf);
                }
            }
        }

        return guardRailJobSession;
    }

    protected GuardRailJobSession(CqlSession sourceSession, SparkConf sparkConf) {
        this.sourceSession = sourceSession;
        batchSize = new Integer(sparkConf.get("spark.migrate.batchSize", "1"));
        printStatsAfter = new Integer(sparkConf.get("spark.migrate.printStatsAfter", "100000"));
        if (printStatsAfter < 1) {
            printStatsAfter = 100000;
        }

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.readRateLimit", "20000")));

        sourceKeyspaceTable = sparkConf.get("spark.migrate.source.keyspaceTable");

        hasRandomPartitioner = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.hasRandomPartitioner", "false"));
        trimColumnRow = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.trimColumnRow", "false"));
        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.counterTable", "false"));

        counterDeltaMaxIndex = Integer
                .parseInt(sparkConf.get("spark.migrate.source.counterTable.update.max.counter.index", "0"));

        String partionKey = sparkConf.get("spark.migrate.query.cols.partitionKey");
        String idCols = sparkConf.get("spark.migrate.query.cols.id");
        idColTypes = getTypes(sparkConf.get("spark.migrate.query.cols.id.types"));

        String selectCols = sparkConf.get("spark.migrate.query.cols.select");
            String updateSelectMappingStr = sparkConf.get("spark.migrate.source.counterTable.update.select.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }
        sourceSelectCondition = sparkConf.get("spark.migrate.query.cols.select.condition", "");
        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                        + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING");

    }

    public void getData(BigInteger min, BigInteger max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(hasRandomPartitioner ? min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact()));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);

                        if(sourceKeyspaceTable.endsWith("smart_rotations")) {
                            int rowColcnt = GetRowColumnLength(sourceRow, trimColumnRow);
                            if (rowColcnt > 1024 * 1024 * 10) {
                                String adv_id = (String) getData(new MigrateDataType("0"), 0, sourceRow);
                                UUID sm_rot_id = (UUID) getData(new MigrateDataType("9"), 1, sourceRow);
                                logger.error("ThreadID: " + Thread.currentThread().getId() + " - advertiser_id: " + adv_id + " - smart_rotation_id: " + sm_rot_id
                                        + " - rotation_set length: " + rowColcnt);
                                continue;
                            }
                        }

                        if(sourceKeyspaceTable.endsWith("resource_status")) {
                            String resourceAction = (String) getData(new MigrateDataType("0"), 2, sourceRow);
                            if (resourceAction.trim().equalsIgnoreCase("spark.migrate.source.keyspaceFilterColumn"))
                                continue;
                        }

                    }

                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);

                        if (sourceKeyspaceTable.endsWith("smart_rotations")) {
                            int rowColcnt = GetRowColumnLength(sourceRow, trimColumnRow);
                            if (rowColcnt > 1024 * 1024 * 10) {
                                String adv_id = (String) getData(new MigrateDataType("0"), 0, sourceRow);
                                UUID sm_rot_id = (UUID) getData(new MigrateDataType("9"), 1, sourceRow);
                                logger.error("ThreadID: " + Thread.currentThread().getId() + " - advertiser_id: " + adv_id + " - smart_rotation_id: " + sm_rot_id
                                        + " - rotation_set length: " + rowColcnt);
                                continue;
                            }
                        }

                        if (sourceKeyspaceTable.endsWith("resource_status")) {
                            String resourceAction = (String) getData(new MigrateDataType("0"), 2, sourceRow);
                            if (resourceAction.trim().equalsIgnoreCase("spark.migrate.source.keyspaceFilterColumn"))
                                continue;
                        }

                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }

                    }
                }


                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

    }

    private int GetRowColumnLength(Row sourceRow, boolean flag) {
        int i = 0;
        Object colData = getData(new MigrateDataType("6%16"), 2, sourceRow);
        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
        i = colBytes.length;
        if (i > 1024*1024*10)
            return i;
        return i;
    }

    public Long getCounterDelta(Long sourceRow, Long astraRow) {
        return sourceRow - astraRow;
    }

}