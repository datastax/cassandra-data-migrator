package datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        this(originSession, targetSession, sc, false);
    }

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        if (originSession == null) {
            return;
        }

        cqlHelper.setOriginSession(originSession);
        cqlHelper.setTargetSession(targetSession);

        printStatsAfter = propertyHelper.getInteger(KnownProperties.PRINT_STATS_AFTER);
        if (!propertyHelper.meetsMinimum(KnownProperties.PRINT_STATS_AFTER, printStatsAfter, 1)) {
            logger.warn(KnownProperties.PRINT_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.PRINT_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.PRINT_STATS_AFTER, KnownProperties.getDefault(KnownProperties.PRINT_STATS_AFTER));
            printStatsAfter = propertyHelper.getInteger(KnownProperties.PRINT_STATS_AFTER);
        }

        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_LIMIT_READ));
        writeLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_LIMIT_WRITE));
        maxRetries = propertyHelper.getInteger(KnownProperties.MAX_RETRIES);
        maxRetriesRowFailure = propertyHelper.getInteger(KnownProperties.MAX_RETRIES_ROW_FAILURE);

        tokenRangeExceptionDir = propertyHelper.getString(KnownProperties.TOKEN_RANGE_EXCEPTION_DIR);
        rowExceptionDir = propertyHelper.getString(KnownProperties.ROW_EXCEPTION_DIR);
        exceptionFileName = ColumnsKeysTypes.getOriginKeyspaceTable(propertyHelper);

        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- ReadRateLimit: {}", readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: {}", writeLimiter.getRate());

        cqlHelper.initialize();
    }

}
