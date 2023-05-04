package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import datastax.astra.migrate.schema.TypeInfo;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class DiffJobSession extends CopyJobSession {

    private static DiffJobSession diffJobSession;
    private final AtomicLong mismatchCounter = new AtomicLong(0);
    private final AtomicLong missingCounter = new AtomicLong(0);
    private final AtomicLong correctedMissingCounter = new AtomicLong(0);
    private final AtomicLong correctedMismatchCounter = new AtomicLong(0);
    private final AtomicLong validCounter = new AtomicLong(0);
    private final AtomicLong skippedCounter = new AtomicLong(0);
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected boolean autoCorrectMissing;
    protected boolean autoCorrectMismatch;

    private DiffJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc);

        autoCorrectMissing = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.missing", "false"));
        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

        autoCorrectMismatch = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.mismatch", "false"));
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);
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
        logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
        boolean done = false;
        int maxAttempts = maxRetries + 1;
        for (int attempts = 1; attempts <= maxAttempts && !done; attempts++) {
            try {
                // cannot do batching if the writeFilter is greater than 0
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(hasRandomPartitioner ?
                                min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact())
                        .setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));

                Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap = new HashMap<Row, CompletionStage<AsyncResultSet>>();
                StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
                    readLimiter.acquire(1);
                    // do not process rows less than writeTimeStampFilter
                    if (!(writeTimeStampFilter && (getLargestWriteTimeStamp(srcRow) < minWriteTimeStampFilter
                            || getLargestWriteTimeStamp(srcRow) > maxWriteTimeStampFilter))) {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        BoundStatement bSelect = selectFromAstra(astraSelectStatement, srcRow);
                        if (null == bSelect) {
                            skippedCounter.incrementAndGet();
                        } else {
                            CompletionStage<AsyncResultSet> targetRowFuture = astraSession.executeAsync(bSelect);
                            srcToTargetRowMap.put(srcRow, targetRowFuture);
                            if (srcToTargetRowMap.size() > fetchSizeInRows) {
                                diffAndClear(srcToTargetRowMap);
                            }
                        }
                    } else {
                        readCounter.incrementAndGet();
                        skippedCounter.incrementAndGet();
                    }
                });
                diffAndClear(srcToTargetRowMap);
                done = true;
            } catch (Exception e) {
                logger.error("Error occurred during Attempt#: {}", attempts, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
                        Thread.currentThread().getId(), min, max, attempts);
            }
        }

    }

    private void diffAndClear(Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap) {
        for (Row srcRow : srcToTargetRowMap.keySet()) {
            try {
                Row targetRow = srcToTargetRowMap.get(srcRow).toCompletableFuture().get().one();
                diff(srcRow, targetRow);
            } catch (Exception e) {
                logger.error("Could not perform diff for Key: {}", getKey(srcRow, tableInfo), e);
            }
        }
        srcToTargetRowMap.clear();
    }

    public synchronized void printCounts(boolean isFinal) {
        String msg = "ThreadID: " + Thread.currentThread().getId();
        if (isFinal) {
            msg += " Final";
            logger.info("################################################################################################");
        }
        logger.info("{} Read Record Count: {}", msg, readCounter.get());
        logger.info("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
        logger.info("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
        logger.info("{} Missing Record Count: {}", msg, missingCounter.get());
        logger.info("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
        logger.info("{} Valid Record Count: {}", msg, validCounter.get());
        logger.info("{} Skipped Record Count: {}", msg, skippedCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private void diff(Row sourceRow, Row astraRow) {
        if (astraRow == null) {
            missingCounter.incrementAndGet();
            logger.error("Missing target row found for key: {}", getKey(sourceRow, tableInfo));
            //correct data

            if (autoCorrectMissing) {
                astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                correctedMissingCounter.incrementAndGet();
                logger.error("Inserted missing row in target: {}", getKey(sourceRow, tableInfo));
            }

            return;
        }

        String diffData = isDifferent(sourceRow, astraRow);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            logger.error("Mismatch row found for key: {} Mismatch: {}", getKey(sourceRow, tableInfo), diffData);

            if (autoCorrectMismatch) {
                if (isCounterTable) {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, astraRow));
                } else {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                }
                correctedMismatchCounter.incrementAndGet();
                logger.error("Updated mismatch row in target: {}", getKey(sourceRow, tableInfo));
            }

            return;
        }

        validCounter.incrementAndGet();
    }

    private String isDifferent(Row sourceRow, Row astraRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, tableInfo.getAllColumns().size()).parallel().forEach(index -> {
            TypeInfo typeInfo = tableInfo.getColumns().get(index).getTypeInfo();
            Object source = getData(typeInfo, index, sourceRow);
            if (index < tableInfo.getKeyColumns().size()) {
                Optional<Object> optionalVal = handleBlankInPrimaryKey(index, source, typeInfo.getTypeClass(), sourceRow, false);
                if (optionalVal.isPresent()) {
                    source = optionalVal.get();
                }
            }

            Object astra = getData(typeInfo, index, astraRow);

            boolean isDiff = typeInfo.diff(source, astra);
            if (isDiff) {
                if (typeInfo.getTypeClass().equals(UdtValue.class)) {
                    String sourceUdtContent = ((UdtValue) source).getFormattedContents();
                    String astraUdtContent = ((UdtValue) astra).getFormattedContents();
                    if (!sourceUdtContent.equals(astraUdtContent)) {
                        diffData.append("(Index: " + index + " Origin: " + sourceUdtContent + " Target: " + astraUdtContent + ") ");
                    }
                } else {
                    diffData.append("(Index: " + index + " Origin: " + source + " Target: " + astra + ") ");
                }
            }
        });

        return diffData.toString();
    }

}
