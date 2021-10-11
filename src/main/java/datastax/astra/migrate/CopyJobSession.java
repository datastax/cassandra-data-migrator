package datastax.astra.migrate;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.log4j.Logger;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.spark.SparkConf;


/*


CREATE TABLE test.user_activity_details_summary_2019_3 (
    id text,
    id_type text,
    month int,
    category text,
    sub_category text,
    field_value frozen<map<text, text>>,
    sub_field_value frozen<map<text, text>>,
    "01" counter,
    "02" counter,
    "03" counter,
    "04" counter,
    "05" counter,
    "06" counter,
    "07" counter,
    "08" counter,
    "09" counter,
    "10" counter,
    "11" counter,
    "12" counter,
    "13" counter,
    "14" counter,
    "15" counter,
    "16" counter,
    "17" counter,
    "18" counter,
    "19" counter,
    "20" counter,
    "21" counter,
    "22" counter,
    "23" counter,
    "24" counter,
    "25" counter,
    "26" counter,
    "27" counter,
    "28" counter,
    "29" counter,
    "30" counter,
    "31" counter,
    total counter,
    PRIMARY KEY (id, id_type, month, category, sub_category, field_value, sub_field_value)
 */
public class CopyJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static CopyJobSession copyJobSession;


    private PreparedStatement astraInsertStatement;
    private PreparedStatement sourceSelectStatement;

    private PreparedStatement astraSelectStatement;
    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec, reads/sec
    // then do the following to set the values as they are only applicable per JVM (hence spark Executor)...
    //  Rate = Total Throughput (write/read per sec) / Total Executors
    private final RateLimiter readLimiter;
    private final RateLimiter writeLimiter;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong writeCounter = new AtomicLong(0);
    private CqlSession sourceSession;
    private CqlSession astraSession;
    private List<String> dataTypesFromQuery = new ArrayList<String>();

    private Integer batchSize = 1;
    private long writeTimeStampFilter = 0;

    public static CopyJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {


        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(sourceSession,astraSession, sparkConf);
                }
            }
        }
        return copyJobSession;
    }

    private CopyJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {

        this.sourceSession = sourceSession;
        this.astraSession=astraSession;

        batchSize = new Integer(sparkConf.get("spark.migrate.batchSize","1"));

        readLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.readRateLimit","20000")));
        writeLimiter = RateLimiter.create(new Integer(sparkConf.get("spark.migrate.writeRateLimit","40000")));

        String sourceKeyspaceTable = sparkConf.get("spark.migrate.source.keyspaceTable");
        String astraKeyspaceTable = sparkConf.get("spark.migrate.astra.keyspaceTable");


        writeTimeStampFilter = new Long(sparkConf.get("spark.migrate.source.writeTimeStampFilter","0"));
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


        astraInsertStatement = astraSession.prepare(
                "update " + astraKeyspaceTable + " set \"01\"+=? , \"02\"+=? , \"03\"+=? , \"04\"+=? , \"05\"+=? , \"06\"+=? , \"07\"+=? , \"08\"+=? , \"09\"+=? , \"10\"+=? , "
                        + "\"11\"+=? , \"12\"+=? , \"13\"+=? , \"14\"+=? , \"15\"+=? , \"16\"+=? , \"17\"+=? , \"18\"+=? , \"19\"+=? , \"20\"+=? , "
                        + "\"21\"+=? , \"22\"+=? , \"23\"+=? , \"24\"+=? , \"25\"+=? , \"26\"+=? , \"27\"+=? , \"28\"+=? , \"29\"+=? , \"30\"+=? , "
                        +"\"31\"+=? ,  total+=? where id=? and id_type=? and month=? and category=? and sub_category=? and field_value=? and sub_field_value=? ");


    }


    public void getDataAndInsert(Long min, Long max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = 5;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(min, max));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                //cannot do batching if the writeFilter is greater than 0
                if(batchSize==1 || writeTimeStampFilter>0l) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        //only process rows greater than writeTimeStampFilter
                        if(sourceRow.getLong(39)<writeTimeStampFilter){
                            continue;
                        }

                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }
                        if(writeTimeStampFilter>0l){
                            ResultSet astraReadResultSet = astraSession.execute(selectFromAstra(astraSelectStatement,sourceRow));
                            Row astraRow = astraReadResultSet.one();

                            CompletionStage<AsyncResultSet> astraWriteResultSet = astraSession.executeAsync(bindInsert(astraInsertStatement,sourceRow, astraRow));
                            writeResults.add(astraWriteResultSet);

                        }else{
                            CompletionStage<AsyncResultSet> astraWriteResultSet = astraSession.executeAsync(bindInsert(astraInsertStatement,sourceRow));
                            writeResults.add(astraWriteResultSet);
                        }


                        if (writeResults.size() > 1000) {
                            iterateAndClearWriteResults(writeResults,1);
                        }
                    }

                    //clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults,1);

                }else{
                    //
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row row : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }

                        batchStatement = batchStatement.add(bindInsert(astraInsertStatement,row));


                        // if batch threshold is met, send the writes and clear the batch
                        if (batchStatement.size() >= batchSize) {

                            CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                            writeResults.add(writeResultSet);
                            batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);

                        }

                        if (writeResults.size()*batchSize > 1000) {
                            iterateAndClearWriteResults(writeResults,batchSize);
                        }
                    }

                    //clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults,batchSize);


                    // if there are any pending writes because the batchSize threshold was not met, then write and clear them
                    if(batchStatement.size() > 0) {
                        CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                        writeResults.add(writeResultSet);
                        iterateAndClearWriteResults(writeResults,batchStatement.size());
                        batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    }
                }


                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Write Record Count: " + writeCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount,e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }



    }

    private void iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults, int incrementBy) throws Exception{
        for(CompletionStage<AsyncResultSet> writeResult: writeResults){
            //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
            if(writeCounter.addAndGet(incrementBy)%1000==0){
                logger.info("TreadID: " + Thread.currentThread().getId() + " Write Record Count: " + writeCounter.get());
            }
        }
        writeResults.clear();
    }

    private BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow){
        return insertStatement.bind(
                sourceRow.getLong(7), sourceRow.getLong(8), sourceRow.getLong(9), sourceRow.getLong(10), sourceRow.getLong(11),
                sourceRow.getLong(12), sourceRow.getLong(13), sourceRow.getLong(14), sourceRow.getLong(15), sourceRow.getLong(16),
                sourceRow.getLong(17), sourceRow.getLong(18), sourceRow.getLong(19), sourceRow.getLong(20), sourceRow.getLong(21),
                sourceRow.getLong(22), sourceRow.getLong(23), sourceRow.getLong(24), sourceRow.getLong(25), sourceRow.getLong(26),
                sourceRow.getLong(27), sourceRow.getLong(28), sourceRow.getLong(29), sourceRow.getLong(30), sourceRow.getLong(31),
                sourceRow.getLong(32), sourceRow.getLong(33), sourceRow.getLong(34), sourceRow.getLong(35), sourceRow.getLong(36),
                sourceRow.getLong(37), sourceRow.getLong(38),
                sourceRow.getString(0), sourceRow.getString(1), sourceRow.getInt(2), sourceRow.getString(3),
                sourceRow.getString(4), sourceRow.getMap(5, String.class, String.class), sourceRow.getMap(6, String.class, String.class));
    }

    private BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow){
        if(astraRow==null){
            return bindInsert(insertStatement,sourceRow);
        }
        return insertStatement.bind(
                getDelta(7, sourceRow,astraRow), getDelta(8, sourceRow,astraRow), getDelta(9, sourceRow,astraRow), getDelta(10, sourceRow,astraRow), getDelta(11, sourceRow,astraRow),
                getDelta(12, sourceRow,astraRow), getDelta(13, sourceRow,astraRow), getDelta(14, sourceRow,astraRow), getDelta(15, sourceRow,astraRow), getDelta(16, sourceRow,astraRow),
                getDelta(17, sourceRow,astraRow), getDelta(18, sourceRow,astraRow), getDelta(19, sourceRow,astraRow), getDelta(20, sourceRow,astraRow), getDelta(21, sourceRow,astraRow),
                getDelta(22, sourceRow,astraRow), getDelta(23, sourceRow,astraRow), getDelta(24, sourceRow,astraRow), getDelta(25, sourceRow,astraRow), getDelta(26, sourceRow,astraRow),
                getDelta(27, sourceRow,astraRow), getDelta(28, sourceRow,astraRow), getDelta(29, sourceRow,astraRow), getDelta(30, sourceRow,astraRow), getDelta(31, sourceRow,astraRow),
                getDelta(32, sourceRow,astraRow), getDelta(33, sourceRow,astraRow), getDelta(34, sourceRow,astraRow), getDelta(35, sourceRow,astraRow), getDelta(36, sourceRow,astraRow),
                getDelta(37, sourceRow,astraRow), getDelta(38, sourceRow,astraRow),
                sourceRow.getString(0), sourceRow.getString(1), sourceRow.getInt(2), sourceRow.getString(3),
                sourceRow.getString(4), sourceRow.getMap(5, String.class, String.class), sourceRow.getMap(6, String.class, String.class));
    }

    private Long getDelta (int index, Row sourceRow, Row astraRow){
        return sourceRow.getLong(index) - astraRow.getLong(index);
    }

    private BoundStatement selectFromAstra(PreparedStatement selectStatement, Row row){
        return selectStatement.bind(
                row.getString(0), row.getString(1), row.getInt(2), row.getString(3),
                row.getString(4), row.getMap(5, String.class, String.class), row.getMap(6, String.class, String.class));
    }


}