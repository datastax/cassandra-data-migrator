package datastax.astra.migrate;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;



public class CopyJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static CopyJobSession copyJobSession;


    private PreparedStatement astraInsertStatement;
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    protected List<MigrateDataType> insertColTypes = new ArrayList<MigrateDataType>();

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
        super(sourceSession,astraSession,sparkConf);

        String insertCols =  sparkConf.get("spark.migrate.query.cols.insert");
        insertColTypes =  getTypes(sparkConf.get("spark.migrate.query.cols.insert.types"));
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

        if(isCounterTable){
            String counterTableUpdate = sparkConf.get("spark.migrate.source.counterTable.update.cql");
            astraInsertStatement = astraSession.prepare( counterTableUpdate);
        }else {
            isPreserveTTLWritetime = Boolean.parseBoolean(sparkConf.get("spark.migrate.preserveTTLWriteTime","false"));
            if(isPreserveTTLWritetime) {
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ") using TTL ? and TIMESTAMP ?");
            }else{
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")");
            }
        }

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
                        if(writeTimeStampFilter>0l && getLargestWriteTimeStamp(sourceRow)<writeTimeStampFilter){
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



    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow){
        BoundStatement boundInsertStatement = insertStatement.bind();
        int index= 0;
        for(index=0;index<insertColTypes.size();index++){
            MigrateDataType dataType = insertColTypes.get(index);
            boundInsertStatement = boundInsertStatement.set(index,getData(dataType, index, sourceRow),dataType.typeClass);
        }

        if(isPreserveTTLWritetime){
            boundInsertStatement = boundInsertStatement.set(index,getLargestTTL(sourceRow),Integer.class);
            index++;
            boundInsertStatement = boundInsertStatement.set(index,getLargestWriteTimeStamp(sourceRow),Long.class);
        }
        return boundInsertStatement;
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow){
        if(astraRow==null) {
            return bindInsert(insertStatement, sourceRow);
        }else{

            BoundStatement boundInsertStatement = insertStatement.bind();
            for(int index=0;index<insertColTypes.size();index++){
                MigrateDataType dataType = insertColTypes.get(index);
                if(index<=deltaRowMaxIndex){
                    boundInsertStatement = boundInsertStatement.set(index,getCounterDelta(index,sourceRow,astraRow),Long.class);
                }else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, index, sourceRow), dataType.typeClass);
                }
            }

            return boundInsertStatement;


        }
    }


    public Long getCounterDelta (int index, Row sourceRow, Row astraRow){
        return sourceRow.getLong(index) - astraRow.getLong(index);

    }





}