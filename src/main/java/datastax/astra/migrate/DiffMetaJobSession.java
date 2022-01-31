package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.spark_project.jetty.util.ConcurrentHashSet;

import java.util.*;
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
public class DiffMetaJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(DiffMetaJobSession.class);
    private static DiffMetaJobSession diffJobSession;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong diffKeyCounter = new AtomicLong(0);
    private AtomicLong diffPayloadCounter = new AtomicLong(0);
    private AtomicLong correctDataCounter = new AtomicLong(0);
    private AtomicLong correctKeyCounter = new AtomicLong(0);
    private AtomicLong validDiffCounter = new AtomicLong(0);

    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();


    protected PreparedStatement sourceDataSelectStatement;
    protected PreparedStatement astraDataSelectStatement;

    protected PreparedStatement sourceCorrectDataSelectStatement;
    protected PreparedStatement astraCorrectDataInsertStatement;

    protected String sourceDataKeyspaceTable;
    protected String astraDataKeyspaceTable;

    protected long maxWriteTimeStampFilter = 0;

    private Set<String> uniqueDiff = new ConcurrentHashSet<String>();
    private Set<String> processedCorrections = new ConcurrentHashSet<String>();


    public static DiffMetaJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        if (diffJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffMetaJobSession(sourceSession, astraSession, sparkConf);
                }
            }
        }
        return diffJobSession;
    }

    private DiffMetaJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        super(sourceSession, astraSession, sparkConf);


        maxWriteTimeStampFilter = new Long(sparkConf.get("spark.migrate.source.maxWriteTimeStampFilter", "0"));

        sourceDataKeyspaceTable = sparkConf.get("spark.migrate.source.data.keyspaceTable");
        astraDataKeyspaceTable = sparkConf.get("spark.migrate.astra.data.keyspaceTable");



        sourceDataSelectStatement = sourceSession.prepare(
                "select data_id, cylinder, value  from " + sourceDataKeyspaceTable + " where data_id=? limit 1");

        sourceCorrectDataSelectStatement = sourceSession.prepare(
                "select data_id, cylinder, value, ttl(value), writetime(value)  from " + sourceDataKeyspaceTable + " where data_id=?");


        astraDataSelectStatement = astraSession.prepare(
                "select data_id, cylinder, value  from  " + astraDataKeyspaceTable
                        + " where data_id=? and cylinder=? ");


        astraCorrectDataInsertStatement = astraSession.prepare("insert into " + astraDataKeyspaceTable + " (data_id, cylinder, value) VALUES (?,?,?) using TTL ? and TIMESTAMP ?");

        selectColTypes = getTypes(sparkConf.get("spark.migrate.diff.select.types"));

    }


    public void getDataDiffAndCorrect(Long min, Long max) {
        try {
            correctData(getDataAndDiff(min, max));
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal Read Record Count: " + readCounter.get());
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal DiffPayload Count: " + diffPayloadCounter.get());
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal DiffKey Count: " + diffKeyCounter.get());
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal CorrectedKey Count: " + correctKeyCounter.get());
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal Valid Count: " + validDiffCounter.get());
            logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectFinal Corrected Count: " + correctDataCounter.get());

        } catch (Exception e) {
            logger.error("Error Correcting with PartitionRange -- ThreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        }
    }

    private Set<SrcDestKey> getDataAndDiff(Long min, Long max) {
        Set<SrcDestKey> srcDestKeys = new HashSet<SrcDestKey>();
        logger.info("ThreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(min, max).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                //cannot do batching if the writeFilter is greater than 0

                for (Row sourceRow : resultSet) {
                    readLimiter.acquire(1);
                    //do not process rows less than writeTimeStampFilter
                    if (writeTimeStampFilter > 0l && getLargestWriteTimeStamp(sourceRow) < writeTimeStampFilter) {
                        continue;
                    }

                    // if above the maxWriteTimeStamp then skip record to diff
                    if (maxWriteTimeStampFilter > 0l && getLargestWriteTimeStamp(sourceRow) > maxWriteTimeStampFilter) {
                        continue;
                    }

//                    if(getLargestTTL(sourceRow)> 0 ){
//                        continue;
//                    }

                    if (readCounter.incrementAndGet() % 100000 == 0) {
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffPayload Count: " + diffPayloadCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffKey Count: " + diffKeyCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectedKey Count: " + correctKeyCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Valid Count: " + validDiffCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Corrected Count: " + correctDataCounter.get());
                    }

                    ResultSet astraReadResultSet = astraSession.execute(selectFromAstra(astraSelectStatement, sourceRow));
                    Row astraRow = astraReadResultSet.one();
                    //if meta id is found, then extract the row for payload and data id
                    String metaKey = diffMetaId(sourceRow, astraRow);
                    if(metaKey!=null){
                        String srcPayloadId = getIdFromPayload(sourceRow.getString(9));
                        String astraPayloadId = getIdFromPayload(astraRow.getString(9));
                        String uniqueKey = srcPayloadId + "%" + astraPayloadId;
                        if(!uniqueDiff.contains(uniqueKey)) {
                            uniqueDiff.add(uniqueKey);
                            logger.error("ThreadID: " + Thread.currentThread().getId() + " - UniqueKey SrcId: " + srcPayloadId + " AstraId: " + astraPayloadId + " MetaKey: " + metaKey);
                            boolean isDifferent = diffData(srcPayloadId, astraPayloadId, 0, metaKey, false);
                            isDifferent = isDifferent || diffData(srcPayloadId, astraPayloadId, 1, metaKey, false);

                            int count =0 ;
                            for(int i=1; i < 30 && !isDifferent;i++ ){
                                for (int j = 1; j <= 4 && !isDifferent; j++) {
                                    isDifferent = isDifferent || diffData(srcPayloadId, astraPayloadId, getRandomInteger(( (i*1000)-1000), i*1000), metaKey, false);
                                }
                            }

                            for(int i=30; i < 100 && !isDifferent;i++ ){
                                isDifferent = isDifferent || diffData(srcPayloadId, astraPayloadId, getRandomInteger(( (i*1000)-1000), i*1000), metaKey, false);
                            }

                            if (isDifferent) {
                                srcDestKeys.add(new SrcDestKey(srcPayloadId, astraPayloadId, metaKey));
                                logger.error("ThreadID: " + Thread.currentThread().getId() + " - Data difference found-  SrcId: " + srcPayloadId + " AstraId: " + astraPayloadId + " MetaKey: " + metaKey);
                            }else{
                                validDiffCounter.incrementAndGet();
                            }
                        }


                    }

                }

//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal Read Record Count: " + readCounter.get());
//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal DiffPayload Count: " + diffPayloadCounter.get());
//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal DiffKey Count: " + diffKeyCounter.get());
//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal CorrectedKey Count: " + correctKeyCounter.get());
//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal Final Valid Count: " + validDiffCounter.get());
//                logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffFinal Corrected Count: " + correctDataCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- ThreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }


        }

        return srcDestKeys;
    }


    public static int getRandomInteger(int maximum, int minimum){ return ((int) (Math.random()*(maximum - minimum))) + minimum; }



    public static void main(String[] args){
//        System.out.println(getIdFromPayload(""));

        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(1,5000));
        }
        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(5000,10000));
        }

        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(10000,20000));
        }

        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(20000,30000));
        }

        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(30000,50000));
        }

        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(50000,100000));
        }

    }



    private static String getIdFromPayload(String payloadStr){
//        payloadStr = "{\"data_id\":\"ec6cdd20-e9a5-4331-4eba-6d8e9da202b0\",\"data_index\":68,\"data_id_write_length\":20,\"col_width\":2880,\"row_length\":1441,\"tile_size\":2,\"blob\":{\"type\":\"GRIB2\",\"product_size\":1884952,\"blob_name\":\"/local-project/tmp/wxgrid/hourly/250/1/2033/1613977200/1613977200.250.1.157.2033.8.1.2.61.1.0.-1.0.3600.4.GRIB2\",\"field_index\":0,\"product_offset\":0}}";
        String id = payloadStr.split("\"")[3];

        return id;
    }
    private String diffMetaId(Row sourceRow, Row astraRow) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, sourceRow));
            } else {
                key.append(" %% " + getData(dataType, index, sourceRow));
            }
        }

        //id is not found
        if (astraRow == null) {
            logger.error("Meta Data is missing in Astra: " + key);
            return null;
        }
        //id is found
        return key.toString();

    }

    private static MigrateDataType blobDataType = new MigrateDataType("7");

    private void correctData(Set<SrcDestKey> srcDestKeys) throws Exception{
        for(SrcDestKey key: srcDestKeys){
            correctData(key.getSrcId(),key.getAstraId(),key.getMetaId());
        }
    }
    private void correctData(String srcPayloadId, String astraPayloadId, String metaKey) throws Exception{
        int noDataCounter = 0;
        int maxIndex = 0;
        for (int i = 0; i <= 150000; i++) {
            maxIndex = i;
            boolean noData = correctData(srcPayloadId, astraPayloadId, i, metaKey, ConsistencyLevel.ONE);

            //Retry with the same CL if there is no data found
            if(noData){
                noData = correctData(srcPayloadId, astraPayloadId, i, metaKey, ConsistencyLevel.LOCAL_QUORUM);
            }

            //increase the CL to ALL if there is no data
            if(noData){
                noData = correctData(srcPayloadId, astraPayloadId, i, metaKey, ConsistencyLevel.ALL);
            }
            //after 3 attempts to data found then increase the counter
            if (noData) {
                noDataCounter++;
            }else{
                noDataCounter=0;
            }
            if (noDataCounter > 5) {
                break;
            }
        }
        correctKeyCounter.incrementAndGet();
        logger.error("ThreadID: " + Thread.currentThread().getId() + "  Corrected for-  SrcId: " + srcPayloadId + " AstraId: " + astraPayloadId + " MaxIndex: " + maxIndex + " MetaKey: " + metaKey);
    }

    private boolean correctData(String srcId, String astrId, Integer index, String metaKey, ConsistencyLevel consistencyLevel) throws Exception {

        boolean noData = true;

        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {


                ResultSet results = sourceSession.execute(sourceCorrectDataSelectStatement.bind(srcId + ":" + index).setConsistencyLevel(consistencyLevel));

                BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);


//        logger.error("Correcting for-  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " MetaKey: " + metaKey);
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();
                for (Row sourceRow : results) {
                    noData = false;
//            logger.error("Corrected difference for-  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " Cylinder: " + sourceRow.getString(1));
                    batchStatement = batchStatement.add(astraCorrectDataInsertStatement.bind(astrId + ":" + index, sourceRow.getString(1), sourceRow.getByteBuffer(2), sourceRow.getInt(3), sourceRow.getLong(4)));
                    if(correctDataCounter.incrementAndGet()%1000000 ==0){
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffPayload Count: " + diffPayloadCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " DiffKey Count: " + diffKeyCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " CorrectedKey Count: " + correctKeyCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Valid Count: " + validDiffCounter.get());
                        logger.info("ThreadID: " + Thread.currentThread().getId() + " Corrected Count: " + correctDataCounter.get());
                    };

                    if (batchStatement.size() >= 10) {
                        writeResults.add(astraSession.executeAsync(batchStatement));
                        batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    }


                    if (writeResults.size() >= 1000) {
                        iterateAndClearWriteResults(writeResults);
                    }



                }

                if (batchStatement.size() > 0) {
                    writeResults.add(astraSession.executeAsync(batchStatement));
                }
                iterateAndClearWriteResults(writeResults);
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with -  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index);
            }
        }
        return noData;
    }

    private void iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults) throws Exception{
        for(CompletionStage<AsyncResultSet> writeResult: writeResults){
            //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
        }
        writeResults.clear();
    }


    private boolean diffData(String srcId, String astrId, Integer index, String metaKey, boolean printLog){
        Row srcRow = sourceSession.execute(sourceDataSelectStatement.bind(srcId+":"+index)).one();
        boolean isDifferent = false;
        if(srcRow!=null) {
            Row astraRow = astraSession.execute(astraDataSelectStatement.bind(astrId + ":" + index, srcRow.getString(1))).one();


            if (srcRow != null && astraRow != null) {
                Object srcPayload = srcRow.getByteBuffer(2);
                Object astraPayload = astraRow.getByteBuffer(2);
                isDifferent = blobDataType.diff(srcPayload,astraPayload );
                if(isDifferent) {

                     logger.error("Data difference found Payload-  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " MetaKey: " + metaKey);

                    diffPayloadCounter.incrementAndGet();
                    //return false on Payload differences
                    return false;
                }
            } else if (srcRow != null && astraRow == null) {
                isDifferent = true;
                if(printLog) {
                    logger.error("Data difference found -  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " MetaKey: " + metaKey);
                }
            }
            if (isDifferent) {
                diffKeyCounter.incrementAndGet();
            }
        }
        return isDifferent;
    }



    private boolean isDifferent(Row sourceRow, Row astraRow, StringBuffer diffData) {

        boolean dataIsDifferent = false;
        if (astraRow == null) {
            dataIsDifferent = true;
            return dataIsDifferent;
        }
        for (int index = 0; index < selectColTypes.size(); index++) {
            MigrateDataType dataType = selectColTypes.get(index);
            Object source = getData(dataType, index, sourceRow);
            Object astra = getData(dataType, index, astraRow);


            boolean isDiff = dataType.diff(source, astra);
            if(isDiff){
                diffData.append(" (Index: " + index + " Source: " + source + " Astra: " + astra + " ) ");
            }
            dataIsDifferent = dataIsDifferent || isDiff;

        }


        return dataIsDifferent;
    }


}
