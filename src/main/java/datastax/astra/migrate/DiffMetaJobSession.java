package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
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
public class DiffMetaJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(DiffMetaJobSession.class);
    private static DiffMetaJobSession diffJobSession;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong diffCounter = new AtomicLong(0);
    private AtomicLong validDiffCounter = new AtomicLong(0);

    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();


    protected PreparedStatement sourceDataSelectStatement;
    protected PreparedStatement astraDataSelectStatement;

    protected PreparedStatement sourceCorrectDataSelectStatement;
    protected PreparedStatement astraCorrectDataInsertStatement;

    protected String sourceDataKeyspaceTable;
    protected String astraDataKeyspaceTable;

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


    public void getDataAndDiff(Long min, Long max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
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

                    if (readCounter.incrementAndGet() % 1000 == 0) {
                        logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        logger.info("TreadID: " + Thread.currentThread().getId() + " Differences Count: " + diffCounter.get());
                        logger.info("TreadID: " + Thread.currentThread().getId() + " Valid Count: " + validDiffCounter.get());
                    }

                    ResultSet astraReadResultSet = astraSession.execute(selectFromAstra(astraSelectStatement, sourceRow));
                    Row astraRow = astraReadResultSet.one();
                    //if meta id is found, then extract the row for payload and data id
                    StringBuffer metaKey = diffMetaId(sourceRow, astraRow);
                    if(metaKey!=null){
                        String srcPayloadId = getIdFromPayload(sourceRow.getString(9));
                        String astraPayloadId = getIdFromPayload(astraRow.getString(9));
                        boolean isDifferent = diffData(srcPayloadId,astraPayloadId,0,metaKey);
                        isDifferent = isDifferent || diffData(srcPayloadId,astraPayloadId,1,metaKey);

                        if(isDifferent) {
                            int noDataCounter = 0;
                            for (int i = 0; i <= 150000; i++) {
                                boolean noData = correctData(srcPayloadId, astraPayloadId, i, metaKey);
                                if (noData) {
                                    noDataCounter++;
                                }
                                if (noDataCounter > 5) {
                                    break;
                                }
                            }

                            diffData(srcPayloadId,astraPayloadId,0,metaKey);
                            diffData(srcPayloadId,astraPayloadId,1,metaKey);

                        }

                        for(int i=1;i<10;i++){
                            diffData(srcPayloadId,astraPayloadId,getRandomInteger(1,10000),metaKey);
                        }
                        for(int i=1;i<5;i++){
                            diffData(srcPayloadId,astraPayloadId,getRandomInteger(10000,30000),metaKey);
                        }
                        for(int i=1;i<2;i++){
                            diffData(srcPayloadId,astraPayloadId,getRandomInteger(30000,50000),metaKey);
                        }
                    }

                }

                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Differences Count: " + diffCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Valid Count: " + validDiffCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }


    }


    public static int getRandomInteger(int maximum, int minimum){ return ((int) (Math.random()*(maximum - minimum))) + minimum; }



    public static void main(String[] args){
//        System.out.println(getIdFromPayload(""));

        for(int i=1;i<10;i++){
            System.out.println(getRandomInteger(1,10000));
        }
        for(int i=1;i<5;i++){
            System.out.println(getRandomInteger(10000,30000));
        }
        for(int i=1;i<2;i++){
            System.out.println(getRandomInteger(30000,50000));
        }
    }



    private static String getIdFromPayload(String payloadStr){
//        payloadStr = "{\"data_id\":\"ec6cdd20-e9a5-4331-4eba-6d8e9da202b0\",\"data_index\":68,\"data_id_write_length\":20,\"col_width\":2880,\"row_length\":1441,\"tile_size\":2,\"blob\":{\"type\":\"GRIB2\",\"product_size\":1884952,\"blob_name\":\"/local-project/tmp/wxgrid/hourly/250/1/2033/1613977200/1613977200.250.1.157.2033.8.1.2.61.1.0.-1.0.3600.4.GRIB2\",\"field_index\":0,\"product_offset\":0}}";
        String id = payloadStr.split("\"")[3];

        return id;
    }
    private StringBuffer diffMetaId(Row sourceRow, Row astraRow) {
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
        return key;

    }

    private static MigrateDataType blobDataType = new MigrateDataType("7");

    private boolean correctData(String srcId, String astrId, Integer index, StringBuffer metaKey) {
        ResultSet results = sourceSession.execute(sourceDataSelectStatement.bind(srcId+":"+index));
        boolean noData = true;

        for(Row sourceRow: results){
            noData = false;
            astraSession.execute(astraCorrectDataInsertStatement.bind(astrId + ":" + index,sourceRow.getString(1),sourceRow.getByteBuffer(2),sourceRow.getInt(3), sourceRow.getLong(4)));
        }

        return noData;
    }
    private boolean diffData(String srcId, String astrId, Integer index, StringBuffer metaKey){
        Row srcRow = sourceSession.execute(sourceDataSelectStatement.bind(srcId+":"+index)).one();
        boolean isDifferent = false;
        if(srcRow!=null) {
            Row astraRow = astraSession.execute(astraDataSelectStatement.bind(astrId + ":" + index, srcRow.getString(1))).one();


            if (srcRow != null && astraRow != null) {
                Object srcPayload = srcRow.getByteBuffer(2);
                Object astraPayload = astraRow.getByteBuffer(2);
                isDifferent = blobDataType.diff(srcPayload,astraPayload );
                if(isDifferent) {
                    logger.error("Data difference found Payload-  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " MetaKey: " + metaKey );
                    diffCounter.incrementAndGet();
                    //return false on Payload differences
                    return false;
                }
            } else if (srcRow != null && astraRow == null) {
                isDifferent = true;
                logger.error("Data difference found -  SrcId: " + srcId + " AstraId: " + astrId + " Index: " + index + " MetaKey: " + metaKey );
            }
            if (isDifferent) {
                diffCounter.incrementAndGet();
            } else {
                validDiffCounter.incrementAndGet();
            }
        }else{
            validDiffCounter.incrementAndGet();
        }
        return isDifferent;
    }
    private void diff(Row sourceRow, Row astraRow) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, sourceRow));
            } else {
                key.append(" %% " + getData(dataType, index, sourceRow));
            }
        }

        if (astraRow == null) {
            logger.error("Data is missing in Astra: " + key);
            return;
        }

        StringBuffer diffData = new StringBuffer();
        boolean isDifferent = isDifferent(sourceRow, astraRow,diffData);

        if (isDifferent) {
            diffCounter.incrementAndGet();
            logger.error("Data difference found -  Key: " + key + " Data: " +  diffData);
            return;
        } else {
            validDiffCounter.incrementAndGet();
        }
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
