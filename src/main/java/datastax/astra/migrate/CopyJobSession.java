package datastax.astra.migrate;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.log4j.Logger;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.spark.SparkConf;



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
    private Integer  maxRetries = 10;
    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong writeCounter = new AtomicLong(0);
    private CqlSession sourceSession;
    private CqlSession astraSession;
    private List<MigrateDataType> idColTypes = new ArrayList<MigrateDataType>();
    private List<MigrateDataType> insertColTypes = new ArrayList<MigrateDataType>();

    private Integer batchSize = 1;
    private long writeTimeStampFilter = 0;
    private List<Integer> writeTimeStampCols = new ArrayList<Integer>();
    private Boolean isCounterTable;
    private Integer deltaRowMaxIndex;

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
        maxRetries = Integer.parseInt(sparkConf.get("spark.migrate.maxRetries","10"));

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


        isCounterTable = Boolean.parseBoolean(sparkConf.get("spark.migrate.source.counterTable","false"));
        String counterTableUpdate = sparkConf.get("spark.migrate.source.counterTable.update");
        deltaRowMaxIndex =  Integer.parseInt(sparkConf.get("spark.migrate.query.cols.counter.deltaRowMaxIndex","0"));

        String writeTimestampColsStr = sparkConf.get("spark.migrate.source.writeTimeStampFilter.cols");
        for(String writeTimeStampCol:writeTimestampColsStr.split(",")){
            writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));

        }


        insertColTypes =  getTypes(sparkConf.get("spark.migrate.query.cols.insert.types"));
        String partionKey =  sparkConf.get("spark.migrate.query.cols.partitionKey");
        String idCols =  sparkConf.get("spark.migrate.query.cols.id");
        idColTypes =  getTypes(sparkConf.get("spark.migrate.query.cols.id.types"));


        String selectCols =  sparkConf.get("spark.migrate.query.cols.select");
        String insertCols =  sparkConf.get("spark.migrate.query.cols.insert");

        String insertBinds = "";
        int count = 1;
        for(String str: insertCols.split(",")){
            if(count>1){
                insertBinds = insertBinds + ",?";
            }else {
                insertBinds = insertBinds + "?";
            }
            count++;
        }

        String idBinds = "";
        count = 1;
        for(String str: idCols.split(",")){
            if(count>1){
                idBinds = idBinds + " and " + str + "= ?";
            }else {
                idBinds = str + "= ?";
            }
            count++;
        }


        sourceSelectStatement = sourceSession.prepare(
                "select " + selectCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim() + ") >= ? and token(" + partionKey.trim() + ") <= ? ALLOW FILTERING");


        astraSelectStatement = astraSession.prepare(
                "select " + selectCols + " from " + astraKeyspaceTable
                        + " where " + idBinds);

        if(isCounterTable){
            astraInsertStatement = astraSession.prepare( counterTableUpdate);
        }else {
            astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")");
        }

    }



    public List<MigrateDataType> getTypes(String types){
        List<MigrateDataType> dataTypes= new ArrayList<MigrateDataType>();
        for(String type:types.split(",")){
            dataTypes.add(new MigrateDataType(type));
        }

        return dataTypes;

    }
    public void getDataAndInsert(Long min, Long max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(min, max));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                //cannot do batching if the writeFilter is greater than 0
                if(batchSize==1 || writeTimeStampFilter>0l) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        //only process rows greater than writeTimeStampFilter
                        if(writeTimeStampFilter>0 && getLargestWriteTimeStamp(sourceRow)<writeTimeStampFilter){
                            continue;
                        }

                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }
                        if(writeTimeStampFilter>0l){
                            Row astraRow = null;
                            if(isCounterTable) {
                                ResultSet astraReadResultSet = astraSession.execute(selectFromAstra(astraSelectStatement, sourceRow));
                                astraRow = astraReadResultSet.one();
                            }



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


    private long getLargestWriteTimeStamp(Row sourceRow){
        long writeTimestamp = 0;
        for(Integer writeTimeStampCol: writeTimeStampCols){
            writeTimestamp = Math.max(writeTimestamp,sourceRow.getLong(writeTimeStampCol));
        }
        return writeTimestamp;
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
        BoundStatement boundInsertStatement = insertStatement.bind();
        for(int index=0;index<insertColTypes.size();index++){
            MigrateDataType dataType = insertColTypes.get(index);
            boundInsertStatement = boundInsertStatement.set(index,getData(dataType, index, sourceRow),dataType.typeClass);
        }
        return boundInsertStatement;
    }

    private BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow){
        if(astraRow==null) {
            return bindInsert(insertStatement, sourceRow);
        }else{

            BoundStatement boundInsertStatement = insertStatement.bind();
            for(int index=0;index<insertColTypes.size();index++){
                MigrateDataType dataType = insertColTypes.get(index);
                if(index<=deltaRowMaxIndex){
                    boundInsertStatement = boundInsertStatement.set(index,getDelta(index,sourceRow,astraRow),Long.class);
                }else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, index, sourceRow), dataType.typeClass);
                }
            }

            return boundInsertStatement;


        }
    }


    private Long getDelta (int index, Row sourceRow, Row astraRow){
        return sourceRow.getLong(index) - astraRow.getLong(index);

    }

    private BoundStatement selectFromAstra(PreparedStatement selectStatement, Row row){
        return selectStatement.bind(
                row.getString(0), row.getString(1), row.getInt(2), row.getString(3),
                row.getString(4), row.getMap(5, String.class, String.class), row.getMap(6, String.class, String.class));
    }



    /*

    TYPES

    0 - String
    1 - Int
    2 - Long
    3 - Double
    4 - Datetime
    5 - Map (Type1, Type2)
    6 - List (Type1)
    -1 : Blob

     */
    private Object getData(MigrateDataType dataType, int index, Row sourceRow){

        if(dataType.typeClass == Map.class){
            return sourceRow.getMap(index,dataType.subTypes.get(0),dataType.subTypes.get(1));
        }else if(dataType.typeClass== List.class){
            return sourceRow.getList(index,dataType.subTypes.get(0));
        }
        return sourceRow.get(index,dataType.typeClass);
    }

    private class MigrateDataType {
        Class typeClass = Object.class;
        List<Class> subTypes = new ArrayList<Class>();

        public MigrateDataType(String dataType){
            if(dataType.contains("%")){
                int count =1 ;
                for(String type: dataType.split("%")){
                    if(count==1){
                        typeClass = getType(Integer.parseInt(type));
                    }else{
                        subTypes.add(getType(Integer.parseInt(type)));
                    }
                    count++;
                }
            }else {
                int type = Integer.parseInt(dataType);
                typeClass = getType(type);
            }
        }


        private Class getType(int type){
            switch(type) {
                case 0:
                    return String.class;

                case 1:
                    return Integer.class;

                case 2:
                    return Long.class;

                case 3:
                    return Double.class;

                case 4:
                    return Instant.class;

                case 5:
                    return Map.class;
                case 6:
                    return List.class;
                case 7:
                    return ByteBuffer.class;

            }

            return Object.class;
        }

    }

}