package datastax.astra.migrate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.apache.log4j.Logger;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.util.concurrent.RateLimiter;


public class CopyJobSession {

    public static Logger logger = Logger.getLogger(CopyJobSession.class);
    private static CopyJobSession copyJobSession;


    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec, reads/sec
    // then do the following to set the values as they are only applicable per JVM (hence spark Executor)...
    //  Rate = Total Throughput (write/read per sec) / Total Executors
    private RateLimiter readLimiter = RateLimiter.create(20000);
    private RateLimiter writeLimiter = RateLimiter.create(40000);

    private CqlSession sourceSession;
    private CqlSession astraSession;
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

    private CopyJobSession(CqlSession sourceSession, CqlSession astraSession) {

        this.sourceSession = sourceSession;
        this.astraSession=astraSession;
        insertStatement = astraSession.prepare(
                "insert into test.sample (key,value) values (?,?)");

        selectStatement = sourceSession.prepare(
                "select key, value from test.sample where token(key) >= ? and token(key) <= ? ALLOW FILTERING");
    }

    public void getDataAndInsert(Long min, Long max) {
        try {

            ResultSet resultSet = sourceSession.execute(selectStatement.bind(min, max));
            Collection<CompletionStage<AsyncResultSet>> writeResults = new ArrayList<CompletionStage<AsyncResultSet>>();


            for (Row row : resultSet) {
                try {
//                    readLimiter.acquire(1);
//                    writeLimiter.acquire(1);

                    //Sample insert query, fill it in with own details
                    CompletionStage<AsyncResultSet> writeResultSet = astraSession.executeAsync(insertStatement.bind(row.getString(0),row.getString(1)));
                    writeResults.add(writeResultSet);

                } catch (Exception e) {
                    logger.error("Error occurred" ,e);
                }
            }


            for(CompletionStage<AsyncResultSet> writeResult: writeResults){
                //wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
                writeResult.toCompletableFuture().get().one();
            }

        } catch (Exception e) {
            System.out.println("Error occurred");
            logger.error("Error occurred" ,e);
        }


    }

}