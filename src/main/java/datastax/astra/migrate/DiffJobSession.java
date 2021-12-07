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
public class DiffJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static DiffJobSession diffJobSession;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong diffCounter = new AtomicLong(0);
    private AtomicLong validDiffCounter = new AtomicLong(0);

    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();

    private long writeTimeStampFilter = 0;

    public static DiffJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        if (diffJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffJobSession(sourceSession, astraSession, sparkConf);
                }
            }
        }
        return diffJobSession;
    }

    private DiffJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        super(sourceSession, astraSession, sparkConf);

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
                    diff(sourceRow, astraRow);

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
