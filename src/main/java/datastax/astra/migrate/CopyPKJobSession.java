package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.cql.CqlHelper;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CopyPKJobSession extends AbstractJobSession {

    private static CopyPKJobSession copyJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected AtomicLong readCounter = new AtomicLong(0);
    protected AtomicLong missingCounter = new AtomicLong(0);
    protected AtomicLong writeCounter = new AtomicLong(0);

    protected CopyPKJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        super(originSession, targetSession, sc, true);
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
                String[] pkFields = row.split(" %% ");
                int idx = 0;
                BoundStatement bspk = cqlHelper.getPreparedStatement(CqlHelper.CQL.ORIGIN_SELECT).bind().setConsistencyLevel(cqlHelper.getReadConsistencyLevel());
                for (MigrateDataType tp : cqlHelper.getIdColTypes()) {
                    bspk = bspk.set(idx, convert(tp.typeClass, pkFields[idx]), tp.typeClass);
                    idx++;
                }
                Row pkRow = cqlHelper.getOriginSession().execute(bspk).one();
                if (null == pkRow) {
                    missingCounter.incrementAndGet();
                    logger.error("Could not find row with primary-key: {}", row);
                    return;
                }
                List<BoundStatement> boundInserts = cqlHelper.bindInsert(cqlHelper.getPreparedStatement(CqlHelper.CQL.TARGET_INSERT), pkRow, null);
                if (null != boundInserts) {
                    for (BoundStatement bs : boundInserts) {
                        ResultSet targetWriteResultSet = cqlHelper.getTargetSession().execute(bs);
                        writeCounter.incrementAndGet();
                        if (readCounter.get() % printStatsAfter == 0) {
                            printCounts(false);
                        }
                    }
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
        logger.info("ThreadID: {} Inserted Record Count: {}", Thread.currentThread().getId(), writeCounter.get());
        if (isFinal) {
            logger.info("################################################################################################");
        }
    }

    private Object convert(Class<?> targetType, String text) {
        PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
        editor.setAsText(text);
        return editor.getValue();
    }

}