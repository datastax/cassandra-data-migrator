package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import datastax.astra.migrate.cql.CqlHelper;
import datastax.astra.migrate.properties.KnownProperties;
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

    private DiffJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);

        autoCorrectMissing = propertyHelper.getBoolean(KnownProperties.TARGET_AUTOCORRECT_MISSING);
        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

        autoCorrectMismatch = propertyHelper.getBoolean(KnownProperties.TARGET_AUTOCORRECT_MISMATCH);
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);
    }

    public static DiffJobSession getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sparkConf) {
        if (diffJobSession == null) {
            synchronized (DiffJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffJobSession(originSession, targetSession, sparkConf);
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
                ResultSet resultSet = cqlHelper.getOriginSession().execute(
                        cqlHelper.getPreparedStatement(CqlHelper.CQL.ORIGIN_SELECT)
                                .bind(cqlHelper.hasRandomPartitioner() ? min : min.longValueExact(),
                                        cqlHelper.hasRandomPartitioner() ? max : max.longValueExact())
                                .setConsistencyLevel(cqlHelper.getReadConsistencyLevel())
                                .setPageSize(cqlHelper.getFetchSizeInRows()));

                Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap = new HashMap<Row, CompletionStage<AsyncResultSet>>();
                StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
                    readLimiter.acquire(1);
                    // do not process rows less than writeTimeStampFilter
                    if (!(cqlHelper.hasWriteTimestampFilter() && (cqlHelper.getLargestWriteTimeStamp(srcRow) < cqlHelper.getMinWriteTimeStampFilter()
                            || cqlHelper.getLargestWriteTimeStamp(srcRow) > cqlHelper.getMaxWriteTimeStampFilter()))) {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        BoundStatement bSelect = cqlHelper.selectFromTargetByPK(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_SELECT_ORIGIN_BY_PK), srcRow);
                        if (null == bSelect) {
                            skippedCounter.incrementAndGet();
                        } else {
                            CompletionStage<AsyncResultSet> targetRowFuture = cqlHelper.getTargetSession().executeAsync(bSelect);
                            srcToTargetRowMap.put(srcRow, targetRowFuture);
                            if (srcToTargetRowMap.size() > cqlHelper.getFetchSizeInRows()) {
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
                logger.error("Could not perform diff for Key: {}", cqlHelper.getKey(srcRow), e);
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

    private void diff(Row originRow, Row targetRow) {
        if (targetRow == null) {
            missingCounter.incrementAndGet();
            logger.error("Missing target row found for key: {}", cqlHelper.getKey(originRow));
            //correct data

            if (autoCorrectMissing) {
                cqlHelper.getTargetSession().execute(cqlHelper.bindInsertOneRow(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), originRow, null));
                correctedMissingCounter.incrementAndGet();
                logger.error("Inserted missing row in target: {}", cqlHelper.getKey(originRow));
            }

            return;
        }

        String diffData = isDifferent(originRow, targetRow);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            logger.error("Mismatch row found for key: {} Mismatch: {}", cqlHelper.getKey(originRow), diffData);

            if (autoCorrectMismatch) {
                if (cqlHelper.isCounterTable()) {
                    cqlHelper.getTargetSession().execute(cqlHelper.bindInsertOneRow(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), originRow, targetRow));
                } else {
                    cqlHelper.getTargetSession().execute(cqlHelper.bindInsertOneRow(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), originRow, null));
                }
                correctedMismatchCounter.incrementAndGet();
                logger.error("Updated mismatch row in target: {}", cqlHelper.getKey(originRow));
            }

            return;
        }

        validCounter.incrementAndGet();
    }

    private String isDifferent(Row originRow, Row targetRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, cqlHelper.getOriginColTypes().size()).parallel().forEach(index -> {
            MigrateDataType dataTypeObj = cqlHelper.getOriginColTypes().get(index);
            Object origin = cqlHelper.getData(dataTypeObj, index, originRow);
            if (index < cqlHelper.getIdColTypes().size()) {
                Optional<Object> optionalVal = cqlHelper.handleBlankInPrimaryKey(index, origin, dataTypeObj.typeClass, originRow, false);
                if (optionalVal.isPresent()) {
                    origin = optionalVal.get();
                }
            }

            Object target = cqlHelper.getData(dataTypeObj, index, targetRow);

            boolean isDiff = dataTypeObj.diff(origin, target);
            if (isDiff) {
                if (dataTypeObj.typeClass.equals(UdtValue.class)) {
                    String originUdtContent = ((UdtValue) origin).getFormattedContents();
                    String targetUdtContent = ((UdtValue) target).getFormattedContents();
                    if (!originUdtContent.equals(targetUdtContent)) {
                        diffData.append("(Index: " + index + " Origin: " + originUdtContent + " Target: " + targetUdtContent + ") ");
                    }
                } else {
                    diffData.append("(Index: " + index + " Origin: " + origin + " Target: " + target + ") ");
                }
            }
        });

        return diffData.toString();
    }

}
