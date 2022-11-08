package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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

    protected CopyPKJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        super(sourceSession, astraSession, sc, true);
    }

    public static CopyPKJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        if (copyJobSession == null) {
            synchronized (CopyPKJobSession.class) {
                if (copyJobSession == null) {
                    copyJobSession = new CopyPKJobSession(sourceSession, astraSession, sc);
                }
            }
        }

        return copyJobSession;
    }

    public void getRowAndInsert(List<SplitPartitions.PKRows> rowsList) {
        for (SplitPartitions.PKRows rows : rowsList) {
            for (String row : rows.pkRows) {
                readCounter.incrementAndGet();
                String[] pkFields = row.split(" %% ");
                int idx = 0;
                BoundStatement bspk = sourceSelectStatement.bind();
                for (MigrateDataType tp : idColTypes) {
                    bspk = bspk.set(idx, convert(tp.typeClass, pkFields[idx]), tp.typeClass);
                    idx++;
                }
                Row pkRow = sourceSession.execute(bspk).one();
                if (null == pkRow) {
                    missingCounter.incrementAndGet();
                    logger.error("Could not find row with primary-key: " + row);
                    continue;
                }
                ResultSet astraWriteResultSet = astraSession
                        .execute(bindInsert(astraInsertStatement, pkRow, null));
                writeCounter.incrementAndGet();

            }
        }

        logger.info("################################################################################################");
        logger.info("TreadID: " + Thread.currentThread().getId() + " Read Record Count: " + readCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " Read Missing Count: " + missingCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " Inserted Record Count: " + writeCounter.get());
        logger.info("################################################################################################");
    }

    private Object convert(Class<?> targetType, String text) {
        PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
        editor.setAsText(text);
        return editor.getValue();
    }

}