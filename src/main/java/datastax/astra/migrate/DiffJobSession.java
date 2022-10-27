package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class DiffJobSession extends CopyJobSession {

    private static DiffJobSession diffJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected Boolean autoCorrectMissing = false;
    protected Boolean autoCorrectMismatch = false;
    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong mismatchCounter = new AtomicLong(0);
    private AtomicLong missingCounter = new AtomicLong(0);
    private AtomicLong correctedMissingCounter = new AtomicLong(0);
    private AtomicLong correctedMismatchCounter = new AtomicLong(0);
    private AtomicLong validCounter = new AtomicLong(0);
    private AtomicLong skippedCounter = new AtomicLong(0);

    private DiffJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc);

        autoCorrectMissing = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.missing", "false"));
        logger.info("PARAM -- Autocorrect Missing: " + autoCorrectMissing);

        autoCorrectMismatch = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.mismatch", "false"));
        logger.info("PARAM -- Autocorrect Mismatch: " + autoCorrectMismatch);
    }

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

    public void getDataAndDiff(BigInteger min, BigInteger max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                // cannot do batching if the writeFilter is greater than 0
                ResultSet resultSet = sourceSession.execute(
                        sourceSelectStatement.bind(hasRandomPartitioner ? min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact()).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

                Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap = new HashMap<Row, CompletionStage<AsyncResultSet>>();
                StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
                    readLimiter.acquire(1);
                    // do not process rows less than writeTimeStampFilter
                    if (!(writeTimeStampFilter && (getLargestWriteTimeStamp(srcRow) < minWriteTimeStampFilter
                            || getLargestWriteTimeStamp(srcRow) > maxWriteTimeStampFilter))) {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts("Current");
                        }

                        CompletionStage<AsyncResultSet> targetRowFuture = astraSession
                                .executeAsync(selectFromAstra(astraSelectStatement, srcRow));
                        srcToTargetRowMap.put(srcRow, targetRowFuture);
                        if (srcToTargetRowMap.size() > 1000) {
                            diffAndClear(srcToTargetRowMap);
                        }
                    } else {
                        readCounter.incrementAndGet();
                        skippedCounter.incrementAndGet();
                    }
                });
                diffAndClear(srcToTargetRowMap);

                printCounts("Final");

                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId()
                        + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

    }

    private void diffAndClear(Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap) {
        for (Row srcRow : srcToTargetRowMap.keySet()) {
            try {
                Row targetRow = srcToTargetRowMap.get(srcRow).toCompletableFuture().get().one();
                diff(srcRow, targetRow);
            } catch (Exception e) {
                logger.error("Could not perform diff for Key: " + getKey(srcRow), e);
            }
        }
        srcToTargetRowMap.clear();
    }

    public void printCounts(String finalStr) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Record Count: "
                + readCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Mismatch Count: "
                + mismatchCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Corrected Mismatch Count: "
                + correctedMismatchCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Missing Count: "
                + missingCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Corrected Missing Count: "
                + correctedMissingCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Valid Count: "
                + validCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Skipped Count: "
                + skippedCounter.get());
    }

    private void diff(Row sourceRow, Row astraRow) {
        if (astraRow == null) {
            missingCounter.incrementAndGet();
            logger.error("Data is missing in Astra: " + getKey(sourceRow));
            //correct data

            if (autoCorrectMissing) {
                astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                correctedMissingCounter.incrementAndGet();
                logger.error("Corrected missing data in Astra: " + getKey(sourceRow));
            }

            return;
        }

        String diffData = isDifferent(sourceRow, astraRow);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            logger.error("Data mismatch found -  Key: " + getKey(sourceRow) + " Data: " + diffData);

            if (autoCorrectMismatch) {
                if (isCounterTable) {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, astraRow));
                } else {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                }
                correctedMismatchCounter.incrementAndGet();
                logger.error("Corrected mismatch data in Astra: " + getKey(sourceRow));
            }

            return;
        }

        validCounter.incrementAndGet();
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
