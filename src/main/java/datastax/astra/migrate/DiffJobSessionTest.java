package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;


/*

(
    data_id text,
    cylinder text,
    value blob,
    PRIMARY KEY (data_id, cylinder)
)

 */
public class DiffJobSessionTest {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static DiffJobSessionTest diffJobSession;


    private PreparedStatement sourceSelectStatement;

    private PreparedStatement astraSelectStatement;
    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec, reads/sec
    // then do the following to set the values as they are only applicable per JVM (hence spark Executor)...
    //  Rate = Total Throughput (write/read per sec) / Total Executors
    private final RateLimiter readLimiter;
    private final RateLimiter writeLimiter;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong diffCounter = new AtomicLong(0);
    private AtomicLong validDiffCounter = new AtomicLong(0);

    private CqlSession sourceSession;
    private CqlSession astraSession;
    private List<String> dataTypesFromQuery = new ArrayList<String>();

    private Integer batchSize = 1;
    private long writeTimeStampFilter = 0;
    private long diffVariance = 0;
    private long ttlDiffVariance = 0;

    public static DiffJobSessionTest getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {


        if (diffJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffJobSessionTest(sourceSession,astraSession, sparkConf);
                }
            }
        }
        return diffJobSession;
    }

    private DiffJobSessionTest(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {


        this.sourceSession = sourceSession;
        this.astraSession=astraSession;

        batchSize = new Integer(sparkConf.get("spark.migrate.batchSize","1"));

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.readRateLimit","20000")));
        writeLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.writeRateLimit","40000")));

        String sourceKeyspaceTable = sparkConf.get("spark.migrate.source.keyspaceTable");
        String astraKeyspaceTable = sparkConf.get("spark.migrate.astra.keyspaceTable");


        writeTimeStampFilter = new Long(sparkConf.get("spark.migrate.source.writeTimeStampFilter","0"));
        diffVariance = new Long(sparkConf.get("spark.migrate.writetime.diffVariance","500000"));
        ttlDiffVariance = new Long(sparkConf.get("spark.migrate.ttl.diffVariance","60"));
        //batchsize set to 1 if there is a writeFilter
        if(writeTimeStampFilter>0){
            batchSize=1;
        }
        logger.info(" DEFAULT -- Write Batch Size: " + batchSize);
        logger.info(" DEFAULT -- Source Keyspace Table: " + sourceKeyspaceTable);
        logger.info(" DEFAULT -- Astra Keyspace Table: " + astraKeyspaceTable);
        logger.info(" DEFAULT -- ReadRateLimit: " + readLimiter.getRate());
        logger.info(" DEFAULT -- WriteRateLimit: " + writeLimiter.getRate());
        logger.info(" DEFAULT -- WriteTimestampFilter: " + writeTimeStampFilter);



        sourceSelectStatement = sourceSession.prepare(
                "select data_id,cylinder,value, writetime(value), ttl(value) from " + sourceKeyspaceTable + " where token(data_id) >= ? and token(data_id) <= ? ALLOW FILTERING");


        astraSelectStatement = astraSession.prepare(
                "select data_id,cylinder,value, writetime(value), ttl(value) from " + astraKeyspaceTable
                        + " where data_id = ? and cylinder=?");
    }


    public void getDataAndDiff(Long min, Long max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = 5;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(min, max).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                //cannot do batching if the writeFilter is greater than 0

                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        //do not process rows less than writeTimeStampFilter
                        if(writeTimeStampFilter>0l && sourceRow.getLong(5)<writeTimeStampFilter){
                            continue;
                        }

                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Differences Count: " + diffCounter.get());
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Valid Count: " + validDiffCounter.get());
                        }

                        ResultSet astraReadResultSet = astraSession.execute(selectFromAstra(astraSelectStatement,sourceRow));
                        Row astraRow = astraReadResultSet.one();
                        diff(sourceRow,astraRow);

                        }

                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Differences Count: " + diffCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Valid Count: " + validDiffCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount,e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }



    }

    private BoundStatement selectFromAstra(PreparedStatement selectStatement, Row row){
        return selectStatement.bind(
                row.getString(0), row.getString(1));
    }

    private void diff(Row sourceRow, Row astraRow){
        String key = "Data id: " + sourceRow.getString(0) + " Cylinder: " + sourceRow.getString(1);

        if(astraRow==null){
            logger.error("Data is missing in Astra: " + key);
            return;
        }


        boolean isDifferent = false;
        isDifferent=isDifferent || sourceRow.getString(0).compareTo(astraRow.getString(0))!=0;
        isDifferent = isDifferent || sourceRow.getString(1).compareTo(astraRow.getString(1))!=0;
        isDifferent = isDifferent || !sourceRow.getByteBuffer(2).equals(sourceRow.getByteBuffer(2));
        if(Math.abs(sourceRow.getLong(3)-astraRow.getLong(3)) > diffVariance){
            isDifferent = true;
        }

        if(Math.abs(sourceRow.getInt(4)-astraRow.getInt(4)) > ttlDiffVariance){
            isDifferent=true;
        }

        if(isDifferent){
            diffCounter.incrementAndGet();
                logger.error("Data difference found: " + key);
            return;
        }else{
            validDiffCounter.incrementAndGet();
        }
    }

}
