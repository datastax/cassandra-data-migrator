package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import datastax.astra.migrate.cql.EnhancedPK;
import datastax.astra.migrate.cql.PKFactory;
import datastax.astra.migrate.cql.Record;
import datastax.astra.migrate.cql.features.ExplodeMap;
import datastax.astra.migrate.cql.features.Feature;
import datastax.astra.migrate.cql.features.FeatureFactory;
import datastax.astra.migrate.cql.features.Featureset;
import datastax.astra.migrate.cql.statements.OriginSelectByPartitionRangeStatement;
import datastax.astra.migrate.cql.statements.TargetSelectByPKStatement;
import datastax.astra.migrate.properties.KnownProperties;
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
    private final AtomicLong readCounter = new AtomicLong(0);
    private final AtomicLong mismatchCounter = new AtomicLong(0);
    private final AtomicLong missingCounter = new AtomicLong(0);
    private final AtomicLong correctedMissingCounter = new AtomicLong(0);
    private final AtomicLong correctedMismatchCounter = new AtomicLong(0);
    private final AtomicLong validCounter = new AtomicLong(0);
    private final AtomicLong skippedCounter = new AtomicLong(0);

    private final boolean isCounterTable;
    private final List<Integer> targetToOriginColumnIndexes;
    private final List<MigrateDataType> targetColumnTypes;
    private final int explodeMapKeyIndex;
    private final int explodeMapValueIndex;

    private DiffJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);

        autoCorrectMissing = propertyHelper.getBoolean(KnownProperties.TARGET_AUTOCORRECT_MISSING);
        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

        autoCorrectMismatch = propertyHelper.getBoolean(KnownProperties.TARGET_AUTOCORRECT_MISMATCH);
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);

        this.isCounterTable = cqlHelper.isCounterTable();
        this.targetToOriginColumnIndexes = cqlHelper.getPKFactory().getTargetToOriginColumnIndexes();
        this.targetColumnTypes = cqlHelper.getPKFactory().getTargetColumnTypes();

        Feature explodeMapFeature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
        if (FeatureFactory.isEnabled(explodeMapFeature)) {
            List<String> targetColumnNames = propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES);
            this.explodeMapKeyIndex = targetColumnNames.indexOf(explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME));
            this.explodeMapValueIndex = targetColumnNames.indexOf(explodeMapFeature.getString(ExplodeMap.Property.VALUE_COLUMN_NAME));
        }
        else {
            this.explodeMapKeyIndex = -1;
            this.explodeMapValueIndex = -1;
        }
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
                PKFactory pkFactory = cqlHelper.getPKFactory();
                OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = cqlHelper.getOriginSelectByPartitionRangeStatement();
                ResultSet resultSet = originSelectByPartitionRangeStatement.execute(originSelectByPartitionRangeStatement.bind(min, max));
                TargetSelectByPKStatement targetSelectByPKStatement = cqlHelper.getTargetSelectByPKStatement();

                List<Record> recordsToDiff = new ArrayList<>(cqlHelper.getFetchSizeInRows());
                StreamSupport.stream(resultSet.spliterator(), false).forEach(originRow -> {
                    readLimiter.acquire(1);
                    Record record = new Record(pkFactory.getTargetPK(originRow), originRow, null);

                    if (originSelectByPartitionRangeStatement.shouldFilterRecord(record)) {
                        readCounter.incrementAndGet();
                        skippedCounter.incrementAndGet();
                    }
                    else {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {printCounts(false);}
                        for (Record r : pkFactory.toValidRecordList(record)) {
                            CompletionStage<AsyncResultSet> targetResult = targetSelectByPKStatement.getAsyncResult(r.getPk());

                            if (null==targetResult) {
                                skippedCounter.incrementAndGet();
                            }
                            else {
                                r.setAsyncTargetRow(targetResult);
                                recordsToDiff.add(r);
                                if (recordsToDiff.size() > cqlHelper.getFetchSizeInRows()) {
                                    diffAndClear(recordsToDiff);
                                }
                            } // targetRecord!=null
                        } // recordSet iterator
                    } // shouldFilterRecord
                });
                diffAndClear(recordsToDiff);
                done = true;
            } catch (Exception e) {
                logger.error("Error occurred during Attempt#: {}", attempts, e);
                logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Attempt# {}",
                        Thread.currentThread().getId(), min, max, attempts);
            }
        }
    }

    private void diffAndClear(List<Record> recordsToDiff) {
        for (Record record : recordsToDiff) {
            try {
                diff(record);
            } catch (Exception e) {
                logger.error("Could not perform diff for key {}: {}", record.getPk(), e);
            }
        }
        recordsToDiff.clear();
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

    private void diff(Record record) {
        EnhancedPK originPK = record.getPk();
        Row originRow = record.getOriginRow();
        Row targetRow = record.getTargetRow();

        if (targetRow == null) {
            missingCounter.incrementAndGet();
            logger.error("Missing target row found for key: {}", record.getPk());

            //correct data
            if (autoCorrectMissing) {
                writeLimiter.acquire(1);
                if (isCounterTable) cqlHelper.getTargetUpdateStatement().putRecord(record);
                else cqlHelper.getTargetInsertStatement().putRecord(record);
                correctedMissingCounter.incrementAndGet();
                logger.error("Inserted missing row in target: {}", record.getPk());
            }
            return;
        }

        String diffData = isDifferent(originPK, originRow, targetRow);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            logger.error("Mismatch row found for key: {} Mismatch: {}", record.getPk(), diffData);

            if (autoCorrectMismatch) {
                writeLimiter.acquire(1);
                if (isCounterTable) cqlHelper.getTargetUpdateStatement().putRecord(record);
                else cqlHelper.getTargetInsertStatement().putRecord(record);
                correctedMismatchCounter.incrementAndGet();
                logger.error("Corrected mismatch row in target: {}", record.getPk());
            }
        }
        else {
            validCounter.incrementAndGet();
        }
    }

    private String isDifferent(EnhancedPK pk, Row originRow, Row targetRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, targetColumnTypes.size()).parallel().forEach(targetIndex -> {
            MigrateDataType dataTypeObj = targetColumnTypes.get(targetIndex);
            Object target = cqlHelper.getData(dataTypeObj, targetIndex, targetRow);

            Object origin;
            if (targetIndex == explodeMapKeyIndex) origin = pk.getExplodeMapKey();
            else if (targetIndex == explodeMapValueIndex) origin = pk.getExplodeMapValue();
            else origin = cqlHelper.getData(dataTypeObj, targetToOriginColumnIndexes.get(targetIndex), originRow);

            boolean isDiff = dataTypeObj.diff(origin, target);
            if (isDiff) {
                if (dataTypeObj.getTypeClass().equals(UdtValue.class)) {
                    String originUdtContent = ((UdtValue) origin).getFormattedContents();
                    String targetUdtContent = ((UdtValue) target).getFormattedContents();
                    if (!originUdtContent.equals(targetUdtContent)) {
                        diffData.append("(Target Index: " + targetIndex + " Origin: " + originUdtContent + " Target: " + targetUdtContent + ") ");
                    }
                } else {
                    diffData.append("Target Index: " + targetIndex + " Origin: " + origin + " Target: " + target + ") ");
                }
            }
        });

        return diffData.toString();
    }

}
