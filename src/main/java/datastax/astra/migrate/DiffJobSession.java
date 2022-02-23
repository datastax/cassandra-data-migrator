package datastax.astra.migrate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

/*
(
    data_id text,
    cylinder text,
    value blob,
    PRIMARY KEY (data_id, cylinder)
)
 */
public class DiffJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(DiffJobSession.class);
    private static DiffJobSession diffJobSession;

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong diffCounter = new AtomicLong(0);
    private AtomicLong validDiffCounter = new AtomicLong(0);

    protected List<MigrateDataType> selectColTypes = new ArrayList<MigrateDataType>();

    public static DiffJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        if (diffJobSession == null) {
            synchronized (DiffJobSession.class) {
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
        ForkJoinPool customThreadPool = new ForkJoinPool();
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                // cannot do batching if the writeFilter is greater than 0
                ResultSet resultSet = sourceSession.execute(
                        sourceSelectStatement.bind(min, max).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

                customThreadPool.submit(() -> {
                    StreamSupport.stream(resultSet.spliterator(), true).forEach(sRow -> {
                        readLimiter.acquire(1);
                        // do not process rows less than writeTimeStampFilter
                        if (!(writeTimeStampFilter > 0l && getLargestWriteTimeStamp(sRow) < writeTimeStampFilter)) {
                            if (readCounter.incrementAndGet() % 1000 == 0) {
                                logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: "
                                        + readCounter.get());
                                logger.info("TreadID: " + Thread.currentThread().getId() + " Differences Count: "
                                        + diffCounter.get());
                                logger.info("TreadID: " + Thread.currentThread().getId() + " Valid Count: "
                                        + validDiffCounter.get());
                            }

                            Row astraRow = astraSession
                                    .execute(selectFromAstra(astraSelectStatement, sRow)).one();
                            diff(sRow, astraRow);
                        }
                    });
                }).get();

                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: "
                        + readCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Differences Count: "
                        + diffCounter.get());
                logger.info(
                        "TreadID: " + Thread.currentThread().getId() + " Final Valid Count: " + validDiffCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId()
                        + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

        customThreadPool.shutdownNow();
    }

    private void diff(Row sourceRow, Row astraRow) {
        if (astraRow == null) {
            logger.error("Data is missing in Astra: " + getKey(sourceRow));
            return;
        }

        String diffData = isDifferent(sourceRow, astraRow);
        if (!diffData.isEmpty()) {
            diffCounter.incrementAndGet();
            logger.error("Data difference found -  Key: " + getKey(sourceRow) + " Data: " + diffData);
            return;
        }

        validDiffCounter.incrementAndGet();
    }

    private String isDifferent(Row sourceRow, Row astraRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, selectColTypes.size()).parallel().forEach(index -> {
            MigrateDataType dataType = selectColTypes.get(index);
            Object source = getData(dataType, index, sourceRow);
            Object astra = getData(dataType, index, astraRow);

            boolean isDiff = dataType.diff(source, astra);
            if (isDiff) {
                diffData.append(" (Index: " + index + " Source: " + source + " Astra: " + astra + " ) ");
            }
        });

        return diffData.toString();
    }

    private String getKey(Row sourceRow) {
        StringBuffer key = new StringBuffer();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, sourceRow));
            } else {
                key.append(" %% " + getData(dataType, index, sourceRow));
            }
        }

        return key.toString();
    }

}
