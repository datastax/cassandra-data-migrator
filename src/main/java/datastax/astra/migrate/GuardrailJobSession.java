package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.schema.TableInfo;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GuardrailJobSession extends BaseJobSession {
    private static GuardrailJobSession guardrailJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected CqlSession session;
    protected TableInfo tableInfo;
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong largeRowCounter = new AtomicLong(0);
    protected AtomicLong largeFieldCounter = new AtomicLong(0);
    protected int guardrailColSizeInKB;

    protected GuardrailJobSession(CqlSession session, SparkConf sc) {
        super(sc);
        this.session = session;

        guardrailColSizeInKB = Integer.parseInt(sc.get("spark.guardrail.colSizeInKB", "0"));
        logger.info("PARAM -- guardrailColSizeInKB: {}", guardrailColSizeInKB);

        tableInfo = TableInfo.getInstance(session, sourceKeyspaceTable.split("\\.")[0],
                sourceKeyspaceTable.split("\\.")[1], Util.getSparkPropOrEmpty(sc, "spark.query.origin"));
        String selectCols = String.join(",", tableInfo.getAllColumns());
        String partitionKey = String.join(",", tableInfo.getPartitionKeyColumns());
        String originSelectQry = "SELECT " + selectCols + " FROM " + sourceKeyspaceTable +
                " WHERE TOKEN(" + partitionKey + ") >= ? AND TOKEN(" + partitionKey + ") <= ?  " +
                sourceSelectCondition + " ALLOW FILTERING";

        logger.info("PARAM -- Detected Table Schema: {}", tableInfo);
        logger.info("PARAM -- Origin select query: {}", originSelectQry);
        sourceSelectStatement = this.session.prepare(originSelectQry);
    }

    public static GuardrailJobSession getInstance(CqlSession session, SparkConf sparkConf) {
        if (guardrailJobSession == null) {
            synchronized (GuardrailJobSession.class) {
                if (guardrailJobSession == null) {
                    guardrailJobSession = new GuardrailJobSession(session, sparkConf);
                }
            }
        }

        return guardrailJobSession;
    }

    public void logIssues(BigInteger min, BigInteger max) {
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        try {
            ResultSet resultSet = session.execute(sourceSelectStatement.bind(hasRandomPartitioner ?
                            min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact())
                    .setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));

            for (Row row : resultSet) {
                readLimiter.acquire(1);
                readCounter.addAndGet(1);
                int largeFieldCnt = 0;
                for (int colIdx = tableInfo.getKeyColumns().size(); colIdx < tableInfo.getAllColumns().size(); colIdx++) {
                    int colSize = getFieldSize(tableInfo.getColumns().get(colIdx).getTypeInfo(), colIdx, row);
                    if (colSize >= (guardrailColSizeInKB * 1024)) {
                        logger.error("ThreadID: {}, PrimaryKey: {}, ColumnName: {} ColumnSize: {}", Thread.currentThread().getId(), getKey(row, tableInfo),
                                tableInfo.getAllColumns().get(colIdx), colSize);
                        largeFieldCnt++;
                    }
                }
                if (largeFieldCnt > 0) {
                    largeRowCounter.addAndGet(1);
                    largeFieldCounter.addAndGet(largeFieldCnt);
                }
                if (readCounter.get() % printStatsAfter == 0) {
                    printCounts(false);
                }
            }
        } catch (Exception e) {
            logger.error("Error occurred ", e);
            logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {}",
                    Thread.currentThread().getId(), min, max);
        }
    }

    public synchronized void printCounts(boolean isFinal) {
        String msg = "ThreadID: " + Thread.currentThread().getId();
        if (isFinal) {
            msg += " Final";
            logger.info("################################################################################################");
        }
        logger.info("{} Read Record Count: {}", msg, readCounter.get());
        logger.info("{} Large Record Count: {}", msg, largeRowCounter.get());
        logger.info("{} Large Field Count: {}", msg, largeFieldCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

}
