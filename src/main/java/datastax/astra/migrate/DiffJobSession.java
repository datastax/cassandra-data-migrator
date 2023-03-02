package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
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

    private Map<CompletionStage<AsyncResultSet>,Object[]> mapTargetResultTrackingMap = new HashMap<>();

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

                Map<Row, Set<CompletionStage<AsyncResultSet>>> srcToTargetRowMap = new HashMap<Row, Set<CompletionStage<AsyncResultSet>>>();
                StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
                    readLimiter.acquire(1);
                    // do not process rows less than writeTimeStampFilter
                    if (!(writeTimeStampFilter && (getLargestWriteTimeStamp(srcRow) < minWriteTimeStampFilter
                            || getLargestWriteTimeStamp(srcRow) > maxWriteTimeStampFilter))) {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts(false);
                        }

                        // targetRowFutureSet contains the target-side future result(s) that correspond to the source row
                        Set<CompletionStage<AsyncResultSet>> targetRowFutureSet = new HashSet<CompletionStage<AsyncResultSet>>();
                        BoundStatement bSelect = null;
                        if (mapToExpandIndex >= 0) {
                            Map colMap = (Map) getData(selectColTypes.get(mapToExpandIndex), mapToExpandIndex, srcRow);

                            // iterate over map elements, inserting one row for each map entry
                            for (Object mapKey : colMap.keySet()) {
                                Object mapValue = colMap.get(mapKey);
                                bSelect = selectFromAstra(astraSelectStatement, srcRow, mapKey);
                                if (null == bSelect) {
                                    continue;
                                }
                                CompletionStage<AsyncResultSet> targetRowFuture = astraSession.executeAsync(bSelect);
                                targetRowFutureSet.add(targetRowFuture);
                                mapTargetResultTrackingMap.put(targetRowFuture,new Object[]{mapKey, mapValue});
                            }
                        } else {
                            bSelect = selectFromAstra(astraSelectStatement, srcRow);
                            if (null != bSelect) {
                                CompletionStage<AsyncResultSet> targetRowFuture = astraSession.executeAsync(bSelect);
                                targetRowFutureSet.add(targetRowFuture);
                            }
                        }
                        if (null == targetRowFutureSet || targetRowFutureSet.isEmpty()) {
                            skippedCounter.incrementAndGet();
                        } else {
                            srcToTargetRowMap.put(srcRow, targetRowFutureSet);
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

// private void diffAndClear(Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap)
    private void diffAndClear(Map<Row, Set<CompletionStage<AsyncResultSet>>> srcToTargetRowMap) {
        for (Row srcRow : srcToTargetRowMap.keySet()) {
            for (CompletionStage<AsyncResultSet> targetRowFuture : srcToTargetRowMap.get(srcRow)) {
                try {
                    AsyncResultSet targetRowSet = targetRowFuture.toCompletableFuture().get();
                    Object mapKey = null;
                    Object mapValue = null;
                    if (null != mapTargetResultTrackingMap && !mapTargetResultTrackingMap.isEmpty()) {
                        mapKey = mapTargetResultTrackingMap.get(targetRowFuture)[0];
                        mapValue = mapTargetResultTrackingMap.get(targetRowFuture)[1];
                    }
                    if (targetRowSet != null) {
                        Row targetRow = targetRowSet.one();
                        diff(srcRow, targetRow, mapKey, mapValue);
                    } else {
                        logger.error("Could not perform diff for Key [targetRowSet or targetRowSet.one() is null]: {}", getKey(srcRow));
                    }
                } catch (Exception e) {
                    logger.error("Could not perform diff for Key [caught Exception]: {}", getKey(srcRow), e);
                }
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

    private void diff(Row sourceRow, Row astraRow, Object mapKey, Object mapValue) {
        if (astraRow == null) {
            missingCounter.incrementAndGet();
            if (null==mapKey) {
                logger.error("Missing target row found for key: {}", getKey(sourceRow));
            } else {
                logger.error("Missing target row found for key: {} mapKey: {}", getKey(sourceRow), mapKey);
            }

            //correct data
            if (autoCorrectMissing) {
                astraSession.execute(bindInsertOneToOne(astraInsertStatement, sourceRow, null, mapKey, mapValue));
                correctedMissingCounter.incrementAndGet();
                if (null==mapKey) {
                    logger.error("Inserted missing row in target: {}", getKey(sourceRow));
                } else {
                    logger.error("Inserted missing row in target: {} mapKey: {}", getKey(sourceRow), mapKey);
                }
            }

            return;
        }

        String diffData = isDifferent(sourceRow, astraRow, mapKey);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            if (null==mapKey) {
                logger.error("Mismatch row found for key: {}", getKey(sourceRow));
            } else {
                logger.error("Mismatch row found for key: {} mapKey: {}", getKey(sourceRow), mapKey);
            }

            if (autoCorrectMismatch) {
                if (isCounterTable) {
                    astraSession.execute(bindInsertOneToOne(astraInsertStatement, sourceRow, astraRow, mapKey, mapValue));
                } else {
                    astraSession.execute(bindInsertOneToOne(astraInsertStatement, sourceRow, null, mapKey, mapValue));
                }
                correctedMismatchCounter.incrementAndGet();
                if (null==mapKey) {
                    logger.error("Updated mismatch row in target: {}", getKey(sourceRow));
                } else {
                    logger.error("Updated mismatch row in target: {} mapKey: {}", getKey(sourceRow), mapKey);
                }
            }

            return;
        }

        validCounter.incrementAndGet();
    }

    private String isDifferent(Row originRow, Row targetRow, Object originMapKey) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, selectColTypes.size()).parallel().forEach(index -> {
            if (mapToExpandIndex < 0 || index < mapToExpandIndex) {
                // if we are not expanding,
                // or index is less than mapToExpandIndex, we have not reached the map column yet; compare as normal
                diffData.append(isDifferent(originRow, index, targetRow, index, null));
            } else if (index == mapToExpandIndex) {
                // compare the key column of the map to the same index position
                diffData.append(isDifferent(originRow, index, targetRow, index, originMapKey));
                // additionally, compare the value column of the map to the next index position in target
                diffData.append(isDifferent(originRow, index, targetRow, index+1, originMapKey));
            } else {
                // if index is greater than mapToExpandIndex, the target column index will be one greater than the source
                diffData.append(isDifferent(originRow, index, targetRow, index+1, null));
            }
        });
        return diffData.toString();
    }

    private String isDifferent(Row originRow, int originIndex, Row targetRow, int targetIndex, Object originMapKey) {
        StringBuffer diffData = new StringBuffer();
        MigrateDataType dataTypeObj = selectColTypes.get(originIndex);
        Object origin = getData(dataTypeObj, originIndex, originRow);
        if (originIndex < idColTypes.size()) {
            Optional<Object> optionalVal = handleBlankInPrimaryKey(originIndex, origin, dataTypeObj.typeClass, originRow, false);
            if (optionalVal.isPresent()) {
                origin = optionalVal.get();
            }
        }

        // If we are expanding a map, origin is going to differ when we are at the map's index.
        // The lack of defensive coding is painful, but refactoring is not in scope :(
        if (originIndex == mapToExpandIndex) {
            if (originIndex == targetIndex) {
                origin = originMapKey;
                dataTypeObj = mapToExpandKeyMigrateDataType;
            } else {
                origin = ((Map<?,?>) origin).get(originMapKey);
                dataTypeObj = mapToExpandValueMigrateDataType;
            }
        }

        Object target = getData(dataTypeObj, targetIndex, targetRow);

        String diffString = isDifferent(dataTypeObj, origin, originIndex, target, targetIndex);
        if (null!= diffString && !diffString.isEmpty()) {
            diffData.append(diffString);
        }

        return diffData.toString();
    }

    private String isDifferent(MigrateDataType dataTypeObj, Object origin, int originIndex, Object target, int targetIndex) {
        if (dataTypeObj.diff(origin, target)) {
            if (dataTypeObj.typeClass.equals(UdtValue.class)) {
                String sourceUdtContent = ((UdtValue) origin).getFormattedContents();
                String astraUdtContent = ((UdtValue) target).getFormattedContents();
                if (!sourceUdtContent.equals(astraUdtContent)) {
                    return "(OriginIndex: " + originIndex + " Origin: " + sourceUdtContent + "TargetIndex: " + targetIndex + " Target: " + astraUdtContent + ") ";
                }
            } else {
                return "(OriginIndex: " + originIndex + " Origin: " + origin + "TargetIndex: " + targetIndex + " Target: " + target + ") ";
            }
        }
        return null;
    }

}
