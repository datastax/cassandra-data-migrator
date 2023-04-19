package datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.data.PKFactory;
import datastax.cdm.data.Record;
import datastax.cdm.feature.*;
import datastax.cdm.cql.statement.OriginSelectByPartitionRangeStatement;
import datastax.cdm.cql.statement.TargetSelectByPKStatement;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
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
    private final boolean forceCounterWhenMissing;
    private final List<Integer> targetToOriginColumnIndexes;
    private final List<String> targetColumnNames;
    private final List<MigrateDataType> targetColumnTypes;
    private final List<MigrateDataType> originColumnTypes;
    private final int explodeMapKeyIndex;
    private final int explodeMapValueIndex;
    protected final MigrateDataType explodeMapKeyDataType;
    protected final MigrateDataType explodeMapValueDataType;
    protected UDTMapper udtMapper;
    protected boolean udtMappingEnabled;

    private final CodecRegistry codecRegistry;

    private DiffJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc);

        autoCorrectMissing = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISSING);
        logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

        autoCorrectMismatch = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISMATCH);
        logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);

        this.isCounterTable = cqlHelper.isCounterTable();
        this.forceCounterWhenMissing = propertyHelper.getBoolean(KnownProperties.AUTOCORRECT_MISSING_COUNTER);
        this.targetToOriginColumnIndexes = ColumnsKeysTypes.getTargetToOriginColumnIndexes(propertyHelper);
        this.targetColumnTypes = ColumnsKeysTypes.getTargetColumnTypes(propertyHelper);
        this.targetColumnNames = ColumnsKeysTypes.getTargetColumnNames(propertyHelper);
        this.originColumnTypes = ColumnsKeysTypes.getOriginColumnTypes(propertyHelper);

        Feature explodeMapFeature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
        if (FeatureFactory.isEnabled(explodeMapFeature)) {
            List<String> targetColumnNames = ColumnsKeysTypes.getTargetColumnNames(propertyHelper);
            this.explodeMapKeyIndex = targetColumnNames.indexOf(explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME));
            this.explodeMapValueIndex = targetColumnNames.indexOf(explodeMapFeature.getString(ExplodeMap.Property.VALUE_COLUMN_NAME));
            this.explodeMapKeyDataType = explodeMapFeature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE);
            this.explodeMapValueDataType = explodeMapFeature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE);
        }
        else {
            this.explodeMapKeyIndex = -1;
            this.explodeMapValueIndex = -1;
            this.explodeMapKeyDataType = null;
            this.explodeMapValueDataType = null;
        }

        this.codecRegistry = cqlHelper.getCodecRegistry();
        this.udtMapper = (UDTMapper) cqlHelper.getFeature(Featureset.UDT_MAPPER);
        this.udtMappingEnabled = FeatureFactory.isEnabled(this.udtMapper);
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
                OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement = cqlHelper.getOriginSelectByPartitionRangeStatement(this.originSession);
                ResultSet resultSet = originSelectByPartitionRangeStatement.execute(originSelectByPartitionRangeStatement.bind(min, max));
                TargetSelectByPKStatement targetSelectByPKStatement = cqlHelper.getTargetSelectByPKStatement(this.targetSession);

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
            if (autoCorrectMissing && isCounterTable && !forceCounterWhenMissing) {
                logger.error("{} is true, but not Inserting as {} is not enabled; key : {}", KnownProperties.AUTOCORRECT_MISSING, KnownProperties.AUTOCORRECT_MISSING_COUNTER, record.getPk());
                return;
            }

            //correct data
            if (autoCorrectMissing) {
                writeLimiter.acquire(1);
                if (isCounterTable) cqlHelper.getTargetUpdateStatement(this.targetSession).putRecord(record);
                else cqlHelper.getTargetInsertStatement(this.targetSession).putRecord(record);
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
                if (isCounterTable) cqlHelper.getTargetUpdateStatement(this.targetSession).putRecord(record);
                else cqlHelper.getTargetInsertStatement(this.targetSession).putRecord(record);
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
            MigrateDataType targetDataTypeObj = targetColumnTypes.get(targetIndex);
            Object target = cqlHelper.getData(targetDataTypeObj, targetIndex, targetRow);

            // If the target contains a UDT, convert it the equivalent UDT in the origin
            if (targetDataTypeObj.hasUDT() && udtMappingEnabled) {
                target = udtMapper.convert(false, targetIndex, target);
            }

            Object origin;
            MigrateDataType originDataTypeObj = null;
            if (targetIndex == explodeMapKeyIndex) {
                origin = pk.getExplodeMapKey();
                originDataTypeObj = explodeMapKeyDataType;
            }
            else if (targetIndex == explodeMapValueIndex) {
                origin = pk.getExplodeMapValue();
                originDataTypeObj = explodeMapValueDataType;
            }
            else {
                int originIndex = targetToOriginColumnIndexes.get(targetIndex);
                if (originIndex < 0)
                    origin = null;
                else {
                    originDataTypeObj = originColumnTypes.get(originIndex);
                    origin = cqlHelper.getData(originDataTypeObj, originIndex, originRow);
                }
            }

            if (null != originDataTypeObj && !originDataTypeObj.equals(targetDataTypeObj)) {
                Object originalOrigin = origin;
                try {
                    origin = MigrateDataType.convert(origin, originDataTypeObj, targetDataTypeObj, codecRegistry);
                }
                catch (Exception e) {
                    logger.error("Error converting data from {} ({}) to {} ({}) for key {} and targetColumn {}; exception: {}",
                            getFormattedContent(originDataTypeObj, originalOrigin), originDataTypeObj.getTypeClass().getName(), getFormattedContent(targetDataTypeObj, target), targetDataTypeObj.getTypeClass().getName(), pk, targetColumnNames.get(targetIndex), e);
                    throw e;
                }
            }

            if (null != origin &&
                targetDataTypeObj.diff(origin, target))  {
                String originContent = getFormattedContent(originDataTypeObj, origin);
                String targetContent = getFormattedContent(targetDataTypeObj, target);
                diffData.append("Target column:").append(targetColumnNames.get(targetIndex)).append(";")
                        .append(" origin[").append(originContent).append("];")
                        .append(" target[").append(targetContent).append("] ");
            }
        });
        return diffData.toString();
    }

    public String getFormattedContent(MigrateDataType migrateDataType, Object value) {
        if (null == value) {
            return "";
        }
        else if (null != migrateDataType && !migrateDataType.hasUDT()) {
            return value.toString();
        }
        else if (value instanceof UdtValue) {
            return ((UdtValue) value).getFormattedContents();
        } else if (value instanceof List || value instanceof Set) {
            Iterator<UdtValue> iterator = ((Iterable<UdtValue>) value).iterator();
            String openBracket;
            String closeBracket;
            if (value instanceof List) {
                openBracket = "[";
                closeBracket = "]";
            }
            else {
                openBracket = "{";
                closeBracket = "}";
            }
            StringBuilder sb = new StringBuilder(openBracket);
            while (iterator.hasNext()) {
                UdtValue udtValue = iterator.next();
                sb.append(udtValue.getFormattedContents());
                if (iterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(closeBracket);
            return sb.toString();
        } else if (value instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            Map<Object,Object> map = (Map<Object,Object>) value;
            if (map.isEmpty()) {
                sb.append("}");
                return sb.toString();
            }
            Map.Entry<Object, Object> oneEntry = map.entrySet().iterator().next();
            boolean keyIsUDT = oneEntry.getKey() instanceof UdtValue;
            boolean valudIsUDT = oneEntry.getValue() instanceof UdtValue;

            int currentElement = 0;
            for (Map.Entry<Object,Object> entry : map.entrySet()) {
                String mapKey = keyIsUDT ? ((UdtValue)entry.getKey()).getFormattedContents() : entry.getKey().toString();
                String mapValue = valudIsUDT ? ((UdtValue)entry.getValue()).getFormattedContents() : entry.getValue().toString();
                if (currentElement++ > 0)
                    sb.append(", ");
                sb.append(mapKey).append("=").append(mapValue);
            }
            sb.append("}");
            return sb.toString();
        }

        // The type contains a UDT, but we don't support the particular structure in which it is contained
        return value.toString();
    }
}
