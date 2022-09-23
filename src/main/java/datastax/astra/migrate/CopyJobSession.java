package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import py4j.Base64;

import java.io.Serializable;
import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class CopyJobSession extends AbstractJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static CopyJobSession copyJobSession;

    protected PreparedStatement astraInsertStatement;
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    protected List<MigrateDataType> insertColTypes = new ArrayList<MigrateDataType>();
    protected List<Integer> updateSelectMapping = new ArrayList<Integer>();
    private Row sourceRow;

    public static CopyJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(sourceSession, astraSession, sparkConf);
                }
            }
        }

        return copyJobSession;
    }

    protected CopyJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
        super(sourceSession, astraSession, sparkConf);

        String insertCols = sparkConf.get("spark.migrate.query.cols.insert");
        insertColTypes = getTypes(sparkConf.get("spark.migrate.query.cols.insert.types"));
        String insertBinds = "";
        int count = 1;
        for (String str : insertCols.split(",")) {
            if (count > 1) {
                insertBinds = insertBinds + ",?";
            } else {
                insertBinds = insertBinds + "?";
            }
            count++;
        }

        if (isCounterTable) {
            String updateSelectMappingStr = sparkConf.get("spark.migrate.source.counterTable.update.select.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = sparkConf.get("spark.migrate.source.counterTable.update.cql");
            astraInsertStatement = astraSession.prepare(counterTableUpdate);
        } else {
            if (isPreserveTTLWritetime) {
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ") using TTL ? and TIMESTAMP ?");
            } else {
                astraInsertStatement = astraSession.prepare("insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")");
            }
        }
    }

    public void getDataAndInsert(BigInteger min, BigInteger max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                ResultSet resultSet = sourceSession.execute(sourceSelectStatement.bind(hasRandomPartitioner ? min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact()));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                // cannot do batching if the writeFilter is greater than 0 or
                // maxWriteTimeStampFilter is less than max long
                // do not batch for counters as it adds latency & increases chance of discrepancy
                if (batchSize == 1 || writeTimeStampFilter || isCounterTable) {
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);

                        if(sourceKeyspaceTable.endsWith("smart_rotations")) {
                            int rowColcnt = GetRowColumnLength(sourceRow, trimColumnRow);
                            if (rowColcnt > 1024 * 1024 * 10) {
                                String adv_id = (String) getData(new MigrateDataType("0"), 0, sourceRow);
                                UUID sm_rot_id = (UUID) getData(new MigrateDataType("9"), 1, sourceRow);
                                logger.error("ThreadID: " + Thread.currentThread().getId() + " - advertiser_id: " + adv_id + " - smart_rotation_id: " + sm_rot_id
                                        + " - rotation_set length: " + rowColcnt);
                                continue;
                            }
                        }

                        if(sourceKeyspaceTable.endsWith("resource_status")) {
                            String resourceAction = (String) getData(new MigrateDataType("0"), 2, sourceRow);
                            if (resourceAction.trim().equalsIgnoreCase("spark.migrate.source.keyspaceFilterColumn"))
                                continue;
                        }

                        if (writeTimeStampFilter) {
                            // only process rows greater than writeTimeStampFilter
                            Long sourceWriteTimeStamp = getLargestWriteTimeStamp(sourceRow);
                            if (sourceWriteTimeStamp < minWriteTimeStampFilter
                                    || sourceWriteTimeStamp > maxWriteTimeStampFilter) {
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: "
                                    + readCounter.get());
                        }
                        Row astraRow = null;
                        if (isCounterTable) {
                            ResultSet astraReadResultSet = astraSession
                                    .execute(selectFromAstra(astraSelectStatement, sourceRow));
                            astraRow = astraReadResultSet.one();
                        }

                        CompletionStage<AsyncResultSet> astraWriteResultSet = astraSession
                                .executeAsync(bindInsert(astraInsertStatement, sourceRow, astraRow));
                        writeResults.add(astraWriteResultSet);
                        if (writeResults.size() > 1000) {
                            iterateAndClearWriteResults(writeResults, 1);
                        }
                    }

                    // clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults, 1);
                } else {
                    BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    for (Row sourceRow : resultSet) {
                        readLimiter.acquire(1);
                        writeLimiter.acquire(1);

                        if(sourceKeyspaceTable.endsWith("smart_rotations")) {
                            int rowColcnt = GetRowColumnLength(sourceRow, trimColumnRow);
                            if (rowColcnt > 1024 * 1024 * 10) {
                                String adv_id = (String) getData(new MigrateDataType("0"), 0, sourceRow);
                                UUID sm_rot_id = (UUID) getData(new MigrateDataType("9"), 1, sourceRow);
                                logger.error("ThreadID: " + Thread.currentThread().getId() + " - advertiser_id: " + adv_id + " - smart_rotation_id: " + sm_rot_id
                                        + " - rotation_set length: " + rowColcnt);
                                continue;
                            }
                        }

                        if(sourceKeyspaceTable.endsWith("resource_status")) {
                            String resourceAction = (String) getData(new MigrateDataType("0"), 2, sourceRow);
                            if (resourceAction.trim().equalsIgnoreCase("spark.migrate.source.keyspaceFilterColumn"))
                                continue;
                        }

                        if (readCounter.incrementAndGet() % 1000 == 0) {
                            logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
                        }
                        batchStatement = batchStatement.add(bindInsert(astraInsertStatement, sourceRow, null));

                        // if batch threshold is met, send the writes and clear the batch
                        if (batchStatement.size() >= batchSize) {
                            CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                            writeResults.add(writeResultSet);
                            batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                        }

                        if (writeResults.size() * batchSize > 1000) {
                            iterateAndClearWriteResults(writeResults, batchSize);
                        }
                    }

                    // clear the write resultset in-case it didnt mod at 1000 above
                    iterateAndClearWriteResults(writeResults, batchSize);

                    // if there are any pending writes because the batchSize threshold was not met, then write and clear them
                    if (batchStatement.size() > 0) {
                        CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(batchStatement);
                        writeResults.add(writeResultSet);
                        iterateAndClearWriteResults(writeResults, batchStatement.size());
                        batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                    }
                }


                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Read Record Count: " + readCounter.get());
                logger.info("TreadID: " + Thread.currentThread().getId() + " Final Write Record Count: " + writeCounter.get());
                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

    }

    private int GetRowColumnLength(Row sourceRow, boolean flag) {
        int i = 0;
        Object colData = getData(new MigrateDataType("6%16"), 2, sourceRow);
        byte[] colBytes = SerializationUtils.serialize((Serializable) colData);
        i = colBytes.length;
        if (i > 1024*1024*10)
            return i;
        return i;
    }

    private void iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults, int incrementBy) throws Exception {
        for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
            //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
            if (writeCounter.addAndGet(incrementBy) % 1000 == 0) {
                logger.info("TreadID: " + Thread.currentThread().getId() + " Write Record Count: " + writeCounter.get());
            }
        }
        writeResults.clear();
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        BoundStatement boundInsertStatement = insertStatement.bind();

        if (isCounterTable) {
            for (int index = 0; index < insertColTypes.size(); index++) {
                MigrateDataType dataType = insertColTypes.get(index);
                // compute the counter delta if reading from astra for the difference
                if (astraRow != null && isCounterTable && index <= counterDeltaMaxIndex) {
                    boundInsertStatement = boundInsertStatement.set(index, getCounterDelta(sourceRow.getLong(updateSelectMapping.get(index)), astraRow.getLong(updateSelectMapping.get(index))), Long.class);
                } else {
                    boundInsertStatement = boundInsertStatement.set(index, getData(dataType, updateSelectMapping.get(index), sourceRow), dataType.typeClass);
                }
            }
        } else {
            int index = 0;
            for (index = 0; index < insertColTypes.size(); index++) {
                MigrateDataType dataTypeObj = insertColTypes.get(index);
                Class dataType = dataTypeObj.typeClass;

                try {
                    Object colData = getData(dataTypeObj, index, sourceRow);
                    if (index < idColTypes.size() && colData == null && dataType == String.class) {
                        colData = "";
                    }
                    boundInsertStatement = boundInsertStatement.set(index, colData, dataType);
                } catch (NullPointerException e) {
                    // ignore the exception for map values being null
                    if (dataType != Map.class) {
                        throw e;
                    }
                }
            }

            if (isPreserveTTLWritetime) {
                boundInsertStatement = boundInsertStatement.set(index, getLargestTTL(sourceRow), Integer.class);
                index++;
                boundInsertStatement = boundInsertStatement.set(index, getLargestWriteTimeStamp(sourceRow), Long.class);
            }
        }

        return boundInsertStatement;
    }

    public Long getCounterDelta(Long sourceRow, Long astraRow) {
        return sourceRow - astraRow;
    }

}