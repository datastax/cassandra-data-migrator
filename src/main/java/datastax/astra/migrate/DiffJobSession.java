package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringUtils;
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
public class DiffJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static DiffJobSession diffJobSession;


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

    public static DiffJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {


        if (diffJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffJobSession(sourceSession,astraSession, sparkConf);
                }
            }
        }
        return diffJobSession;
    }

    private DiffJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {


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
                "select id, id_type, month, category, sub_category, field_value, sub_field_value, \"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\","
                        + "\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\","
                        + "\"21\",\"22\",\"23\",\"24\",\"25\",\"26\",\"27\",\"28\",\"29\",\"30\","
                        +"\"31\", total, writetime(total) from " + sourceKeyspaceTable + " where token(id) >= ? and token(id) <= ? ALLOW FILTERING");


        astraSelectStatement = astraSession.prepare(
                "select id, id_type, month, category, sub_category, field_value, sub_field_value, \"01\",\"02\",\"03\",\"04\",\"05\",\"06\",\"07\",\"08\",\"09\",\"10\","
                        + "\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",\"17\",\"18\",\"19\",\"20\","
                        + "\"21\",\"22\",\"23\",\"24\",\"25\",\"26\",\"27\",\"28\",\"29\",\"30\","
                        +"\"31\", total, writetime(total) from " + astraKeyspaceTable
                        + " where id = ? and id_type=? and month=? and category=? and sub_category=? and field_value=? and sub_field_value=?");
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
                        if(writeTimeStampFilter>0l && sourceRow.getLong(39)<writeTimeStampFilter){
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
                row.getString(0), row.getString(1), row.getInt(2), row.getString(3),
                row.getString(4), row.getMap(5, String.class, String.class), row.getMap(6, String.class, String.class));
    }

    private void diff(Row sourceRow, Row astraRow){



        String key = sourceRow.getString(0)  + " %% " + sourceRow.getString(1) + " %% " + sourceRow.getInt(2) + " %% " +  sourceRow.getString(3) + " %% " +
                sourceRow.getString(4) + " %% " +  sourceRow.getMap(5, String.class, String.class) + " %% " +  sourceRow.getMap(6, String.class, String.class);

        if(astraRow==null){
                logger.error("Data is missing in Astra: " + key);
            return;
        }


        boolean isDifferent = isDifferent(sourceRow,astraRow);

        if(isDifferent){
            diffCounter.incrementAndGet();
                logger.error("Data difference found: " + key);
            return;
        }else{
            validDiffCounter.incrementAndGet();
        }
    }


    private boolean isDifferent(Row sourceRow, Row astraRow){

        boolean dataIsDifferent = false;
        if(astraRow==null){
            dataIsDifferent=true;
        }

        for(int i=7;i<=38;i++){
            long delta = getDelta(i, sourceRow,astraRow);
            if(Math.abs(delta)>0){
                dataIsDifferent=true;
            }
        }


        return dataIsDifferent;
    }

    private Long getDelta (int index, Row sourceRow, Row astraRow){
        return sourceRow.getLong(index) - astraRow.getLong(index);
    }


}
