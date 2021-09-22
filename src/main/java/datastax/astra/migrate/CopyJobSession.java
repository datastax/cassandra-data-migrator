package datastax.astra.migrate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.apache.log4j.Logger;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;


public class CopyJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static CopyJobSession copyJobSession;


    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec, reads/sec
    // then do the following to set the values as they are only applicable per JVM (hence spark Executor)...
    //  Rate = Total Throughput (write/read per sec) / Total Executors
    private RateLimiter readLimiter = RateLimiter.create(5000);
    private RateLimiter writeLimiter = RateLimiter.create(10000);

    private AtomicLong readCounter = new AtomicLong(0);
    private AtomicLong writeCounter = new AtomicLong(0);
    private CqlSession sourceSession;
    private CqlSession destSession;
    public static CopyJobSession getInstance(CqlSession sourceSession, CqlSession astraSession) {

        if (copyJobSession == null) {
            synchronized (CopyJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyJobSession(sourceSession,astraSession);
                }
            }
        }
        return copyJobSession;
    }

    private CopyJobSession(CqlSession sourceSession, CqlSession destSession) {

        this.sourceSession = sourceSession;
        this.destSession=destSession;
        insertStatement = destSession.prepare(
                "insert into xrplreporting.full_history (hash,object) values (?,?)");

        selectStatement = sourceSession.prepare(
                "select hash, object from xrplreporting.full_history where token(hash) >= ? and token(hash) <= ? ALLOW FILTERING");
    }
    public static void test(Long min, Long max) {
        logger.error("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        logger.error("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        logger.error("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        logger.error("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
    }
    public void getDataAndInsert(Long min, Long max) {
        RateLimiter errorLimiter = RateLimiter.create(100);

        while(true)
        {
            int numReadFailures = 0;
            try {
                ResultSet resultSet = sourceSession.execute(selectStatement.bind(min, max));
                Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();

                for (Row row : resultSet) {
                    readLimiter.acquire(1);
                    if(readCounter.incrementAndGet()%1000==0){
                        logger.info("Read Record Count: " + readCounter.get());
                    }
                    int numFailures = 0;
                    while(true)
                    {
                        try
                        {
                            writeLimiter.acquire(1);
                            if(numFailures > 0) {
                                errorLimiter.acquire(Math.min(99,numFailures));
                            }
                            //Sample insert query, fill it in with own details
                            CompletionStage<AsyncResultSet> writeResultSet = destSession.executeAsync(insertStatement.bind(row.getByteBuffer(0),row.getByteBuffer(1)));
                            writeResults.add(writeResultSet);

                            if(writeResults.size()>1000){
                                for(CompletionStage<AsyncResultSet> writeResult: writeResults){
                                    //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
                                    writeResult.toCompletableFuture().get().one();
                                    if(writeCounter.incrementAndGet()%1000==0){
                                        logger.info("Write Record Count: " + writeCounter.get());
                                    }
                                }
                                //clear results
                                writeResults.clear();
                            }
                            break;
                        } catch (Exception e) {
                            numFailures++;
                            logger.error("Write error occurred with partition range " + min + " - " + max + " . num failures = " + numFailures, e);
                            continue;
                        }
                    }
                }
                break;
            } catch (Exception e) {
                ++numReadFailures;
                logger.error("Read error occurred with partition range " + min + " - " + max + " . num read failures = " + numReadFailures, e);
                continue;
            }
        }
        logger.info("Finished partition range " + min + " - " + max);



    }

}
