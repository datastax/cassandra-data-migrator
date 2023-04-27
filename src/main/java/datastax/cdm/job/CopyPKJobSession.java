package datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.data.PKFactory;
import datastax.cdm.data.Record;
import datastax.cdm.cql.statement.OriginSelectByPKStatement;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class CopyPKJobSession extends AbstractJobSession {

    private static CopyPKJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong missingCounter = new AtomicLong(0);
    protected AtomicLong skipCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    private AtomicLong correctedMissingCounter = new AtomicLong(0);
    private AtomicLong correctedMismatchCounter = new AtomicLong(0);
    private AtomicLong validCounter = new AtomicLong(0);
    private AtomicLong mismatchCounter = new AtomicLong(0);
    private AtomicLong skippedCounter = new AtomicLong(0);
    private AtomicLong failedRowCounter = new AtomicLong(0);


    private final PKFactory pkFactory;
    private final List<MigrateDataType> originPKTypes;
    private final boolean isCounterTable;
    private final OriginSelectByPKStatement originSelectByPKStatement;

    protected CopyPKJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc, true);
        pkFactory = cqlHelper.getPKFactory();
        originPKTypes = pkFactory.getPKTypes(PKFactory.Side.ORIGIN);
        isCounterTable = cqlHelper.isCounterTable();
        originSelectByPKStatement = cqlHelper.getOriginSelectByPKStatement();
    }

    public static CopyPKJobSession getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (copyJobSession == null) {
            synchronized (CopyPKJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyPKJobSession(originSession, targetSession, sc);
                }
            }
        }

        return copyJobSession;
    }

    public void getRowAndInsert(List<SplitPartitions.PKRows> rowsList) {
        for (SplitPartitions.PKRows rows : rowsList) {
            rows.pkRows.parallelStream().forEach(row -> {
                readCounter.incrementAndGet();
                EnhancedPK pk = toEnhancedPK(row);
                if (null == pk || pk.isError()) {
                    missingCounter.incrementAndGet();
                    logger.error("Could not build PK object with value <{}>; error is: {}", row, (null == pk ? "null" : pk.getMessages()));
                    return;
                }

                Record recordFromOrigin = originSelectByPKStatement.getRecord(pk);
                if (null == recordFromOrigin) {
                    missingCounter.incrementAndGet();
                    logger.error("Could not find origin row with primary-key: {}", row);
                    return;
                }
                Row originRow = recordFromOrigin.getOriginRow();

                Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);
                if (originSelectByPKStatement.shouldFilterRecord(record)) {
                    skipCounter.incrementAndGet();
                    return;
                }

                writeLimiter.acquire(1);
                if (isCounterTable) cqlHelper.getTargetUpdateStatement().putRecord(record);
                else cqlHelper.getTargetInsertStatement().putRecord(record);
                writeCounter.incrementAndGet();

                if (readCounter.get() % printStatsAfter == 0) {
                    printCounts(false);
                }
            });
        }

        printCounts(true);
    }

    public void printCounts(boolean isFinal) {
        if (isFinal) {
            logger.info("################################################################################################");
        }
        logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
        logger.info("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
        logger.info("ThreadID: {} Skipped Record Count: {}", Thread.currentThread().getId(), skipCounter.get());
        logger.info("ThreadID: {} Inserted Record Count: {}", Thread.currentThread().getId(), writeCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private EnhancedPK toEnhancedPK(String rowString) {
        String[] pkFields = rowString.split(" %% ");
        List<Object> values = new ArrayList<>(originPKTypes.size());
        for (int i=0; i<pkFields.length; i++) {
            PropertyEditor editor = PropertyEditorManager.findEditor(originPKTypes.get(i).getTypeClass());
            editor.setAsText(pkFields[i]);
            values.add(editor.getValue());
        }
        return pkFactory.toEnhancedPK(values, pkFactory.getPKTypes(PKFactory.Side.ORIGIN));
    }
    /*
    // FR: THIS ENTIRE THING NEEDS TO BE MOVED FROM HERE TO DIFFJOBSESSION CLASS
    @SuppressWarnings("unchecked")
    public void getRowAndDiff(List<SplitPartitions.PKRows> rowsList) {
        for (SplitPartitions.PKRows rows : rowsList) {
            rows.pkRows.parallelStream().forEach(row -> {
                readCounter.incrementAndGet();
                EnhancedPK pk = toEnhancedPK(row);
                if (null == pk || pk.isError()) {
                    missingCounter.incrementAndGet();
                    logger.error("Could not build PK object with value <{}>; error is: {}", row, (null == pk ? "null" : pk.getMessages()));
                    return;
                }
                int maxAttempts = maxRetriesRowFailure;
                Row sourceRow = null;
                int diffAttempt = 0;
                for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {
                    try {
                        sourceRow = originSelectByPKStatement.getRecord(pk).getOriginRow();
                        if (sourceRow != null) {
                            Row astraRow = cqlHelper.getTargetSelectByPKStatement().getRecord(pk).getTargetRow();
                            diffAttempt++;
                            diff(sourceRow, astraRow, diffAttempt);
                        } else {
                            logger.error("Could not find row with primary-key: {} on source", row);
                        }
                        retryCount = maxAttempts;
                    } catch (Exception e) {
                        logger.error("Could not find row with primary-key: {} retry# {}", row, retryCount, e);
                        if (retryCount == maxAttempts) {
                            logFailedRecordInFile(sourceRow);
                        }
                    }
                }
            });
        }
        printValidationCounts(true);
    }

    private void diff(Row sourceRow, Row astraRow, int diffAttempt) {
        if (astraRow == null) {
            if (diffAttempt == 1) {
                missingCounter.incrementAndGet();
                logger.info("Missing target row found for key: {}", getKey(sourceRow));
            }
            targetSession.execute(bindInsert(targetInsertStatement, sourceRow, null));
            correctedMissingCounter.incrementAndGet();
            logger.info("Inserted missing row in target: {}", getKey(sourceRow));
        } else {
            String diffData = isDifferent(sourceRow, astraRow);
            if (!diffData.isEmpty()) {
                if (diffAttempt == 1) {
                    mismatchCounter.incrementAndGet();
                    logger.info("Mismatch row found for key: {} Mismatch: {}", getKey(sourceRow), diffData);
                }

                Record record = new Record(pkFactory.getTargetPK(sourceRow), astraRow, null);
                if (isCounterTable) cqlHelper.getTargetUpdateStatement().putRecord(record);
                else cqlHelper.getTargetInsertStatement().putRecord(record);
                correctedMismatchCounter.incrementAndGet();
                logger.info("Updated mismatch row in target: {}", getKey(sourceRow));
            } else {
                validCounter.incrementAndGet();
            }
        }
    }

    private String isDifferent(Row sourceRow, Row astraRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, selectColTypes.size()).parallel().forEach(index -> {
            MigrateDataType dataType = selectColTypes.get(index);
            Object source = getData(dataType, index, sourceRow);
            Object astra = getData(dataType, index, astraRow);

            boolean isDiff = dataType.diff(source, astra);
            if (isDiff) {
                if (dataType.typeClass.equals(UdtValue.class)) {
                    String sourceUdtContent = ((UdtValue) source).getFormattedContents();
                    String astraUdtContent = ((UdtValue) astra).getFormattedContents();
                    if (!sourceUdtContent.equals(astraUdtContent)) {
                        diffData.append("(Index: " + index + " Origin: " + sourceUdtContent + " Target: "
                                + astraUdtContent + ") ");
                    }
                } else {
                    diffData.append("(Index: " + index + " Origin: " + source + " Target: " + astra + ") ");
                }
            }
        });

        return diffData.toString();
    }

    private void logFailedRecordInFile(Row sourceRow) {
        try {
            failedRowCounter.getAndIncrement();
            Util.FileAppend(rowExceptionDir, exceptionFileName, getKey(sourceRow));
            logger.error("Failed to validate row: {} after {} retry.", getKey(sourceRow));
        } catch (Exception exp) {
            logger.error("Error occurred while writing to key {} to file ", getKey(sourceRow), exp);
        }
    }
    */
    public void printValidationCounts(boolean isFinal) {
        String msg = "ThreadID: " + Thread.currentThread().getId();
        if (isFinal) {
            logger.info(
                    "################################################################################################");

            logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
            logger.info("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
            logger.info("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
            logger.info("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
            logger.info("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
            logger.info("{} Skipped Record Count: {}", msg, skippedCounter.get());
            logger.info("{} Failed row Count: {}", msg, failedRowCounter.get());
            logger.info("{} Valid Record Count: {}", msg, validCounter.get());
        }

        logger.debug("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
        logger.debug("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
        logger.debug("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
        logger.debug("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
        logger.debug("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
        logger.debug("{} Skipped Record Count: {}", msg, skippedCounter.get());
        logger.debug("{} Failed row Count: {}", msg, failedRowCounter.get());
        logger.info("{} Valid Record Count: {}", msg, validCounter.get());

        if (isFinal) {
            logger.info(
                    "################################################################################################");
        }
    }
}