package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class CopyJobSession extends AbstractJobSession {

    private static CopyJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    protected CopyJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc);
    }

    public static CopyJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(sourceSession, astraSession, sc);
                }
            }
        }

        return copyJobSession;
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

                        if (writeTimeStampFilter) {
                            // only process rows greater than writeTimeStampFilter
                            Long sourceWriteTimeStamp = getLargestWriteTimeStamp(sourceRow);
                            if (sourceWriteTimeStamp < minWriteTimeStampFilter
                                    || sourceWriteTimeStamp > maxWriteTimeStampFilter) {
                                continue;
                            }
                        }

                        writeLimiter.acquire(1);
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
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
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
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

    private void iterateAndClearWriteResults(Collection<CompletionStage<AsyncResultSet>> writeResults, int incrementBy) throws Exception {
        for (CompletionStage<AsyncResultSet> writeResult : writeResults) {
            //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
            writeResult.toCompletableFuture().get().one();
            if (writeCounter.addAndGet(incrementBy) % printStatsAfter == 0) {
                logger.info("TreadID: " + Thread.currentThread().getId() + " Write Record Count: " + writeCounter.get());
            }
        }
        writeResults.clear();
    }

}