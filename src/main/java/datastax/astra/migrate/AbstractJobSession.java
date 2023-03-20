package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.astra.migrate.properties.KnownProperties;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected CqlHelper cqlHelper;

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
        this(sourceSession, astraSession, sc, false);
    }

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        if (sourceSession == null) {
            return;
        }

        this.sourceSession = sourceSession;
        this.astraSession = astraSession;
        this.cqlHelper = new CqlHelper(this.propertyHelper, this.sourceSession, this.astraSession, isJobMigrateRowsFromFile, this);

        batchSize = propertyHelper.getInteger(KnownProperties.SPARK_BATCH_SIZE);
        fetchSizeInRows = propertyHelper.getInteger(KnownProperties.READ_FETCH_SIZE);

        printStatsAfter = propertyHelper.getInteger(KnownProperties.SPARK_STATS_AFTER);
        if (!propertyHelper.meetsMinimum(KnownProperties.SPARK_STATS_AFTER, printStatsAfter, 1)) {
            logger.warn(KnownProperties.SPARK_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.SPARK_STATS_AFTER, KnownProperties.getDefaultAsString(KnownProperties.SPARK_STATS_AFTER));
        }

        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_READ));
        writeLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.SPARK_LIMIT_WRITE));
        maxRetries = propertyHelper.getInteger(KnownProperties.SPARK_MAX_RETRIES);

        ttlCols = propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_COLS);
        writeTimeStampCols = propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_COLS);

        writeTimeStampFilter = propertyHelper.getBoolean(KnownProperties.ORIGIN_FILTER_WRITETS_ENABLED);
        if (writeTimeStampFilter) {
            batchSize = 1;
            propertyHelper.setProperty(KnownProperties.SPARK_BATCH_SIZE, batchSize);
        }
        minWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MIN);
        maxWriteTimeStampFilter = propertyHelper.getLong(KnownProperties.ORIGIN_FILTER_WRITETS_MAX);

        customWritetime = propertyHelper.getLong(KnownProperties.TARGET_CUSTOM_WRITETIME);
        isCounterTable = propertyHelper.getBoolean(KnownProperties.ORIGIN_IS_COUNTER);

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", batchSize);
        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Read Fetch Size: {}", fetchSizeInRows);
        logger.info("PARAM -- Source Keyspace Table: {}", sourceKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: {}", targetKeyspaceTable);
        logger.info("PARAM -- ReadRateLimit: {}", readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: {}", writeLimiter.getRate());
        logger.info("PARAM -- TTLCols: {}", ttlCols);
        logger.info("PARAM -- WriteTimestampFilterCols: {}", writeTimeStampCols);
        logger.info("PARAM -- WriteTimestampFilter: {}", writeTimeStampFilter);
        if (writeTimeStampFilter) {
            logger.info("PARAM -- minWriteTimeStampFilter: {} datetime is {}", minWriteTimeStampFilter,
                    Instant.ofEpochMilli(minWriteTimeStampFilter / 1000));
            logger.info("PARAM -- maxWriteTimeStampFilter: {} datetime is {}", maxWriteTimeStampFilter,
                    Instant.ofEpochMilli(maxWriteTimeStampFilter / 1000));
        }



        cqlHelper.initialize();
        String fullSelectQuery = cqlHelper.getCqlString(CqlHelper.CqlStatementType.ORIGIN_SELECT);
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", fullSelectQuery);
        sourceSelectStatement = sourceSession.prepare(fullSelectQuery);

        astraSelectStatement = astraSession.prepare(cqlHelper.getCqlString(CqlHelper.CqlStatementType.TARGET_SELECT_BY_PK));

        hasRandomPartitioner = propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);

        astraInsertStatement = astraSession.prepare(cqlHelper.getCqlString(CqlHelper.CqlStatementType.TARGET_INSERT));

        // Handle rows with blank values for 'timestamp' data-type in primary-key fields
        if (null != propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS))
            tsReplaceVal = propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS);
    }

    public BoundStatement bindInsert(PreparedStatement insertStatement, Row sourceRow, Row astraRow) {
        return cqlHelper.bindInsert(insertStatement, sourceRow, astraRow);
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow) {
        return cqlHelper.selectFromTargetByPK(selectStatement, sourceRow);
    }

}
