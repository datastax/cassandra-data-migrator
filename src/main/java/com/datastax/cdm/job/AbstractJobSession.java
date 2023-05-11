package com.datastax.cdm.job;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.properties.KnownProperties;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected EnhancedSession originSession;
    protected EnhancedSession targetSession;

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        this(originSession, targetSession, sc, false);
    }

    protected AbstractJobSession(CqlSession originSession, CqlSession targetSession, SparkConf sc, boolean isJobMigrateRowsFromFile) {
        super(sc);

        if (originSession == null) {
            return;
        }

        printStatsAfter = propertyHelper.getInteger(KnownProperties.PRINT_STATS_AFTER);
        if (!propertyHelper.meetsMinimum(KnownProperties.PRINT_STATS_AFTER, printStatsAfter, 1)) {
            logger.warn(KnownProperties.PRINT_STATS_AFTER +" must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.PRINT_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.PRINT_STATS_AFTER, KnownProperties.getDefault(KnownProperties.PRINT_STATS_AFTER));
            printStatsAfter = propertyHelper.getInteger(KnownProperties.PRINT_STATS_AFTER);
        }

        readLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_LIMIT_READ));
        writeLimiter = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_LIMIT_WRITE));
        maxRetries = propertyHelper.getInteger(KnownProperties.MAX_RETRIES);

        tokenRangeExceptionDir = propertyHelper.getString(KnownProperties.TOKEN_RANGE_EXCEPTION_DIR);
        exceptionFileName = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);

        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- ReadRateLimit: {}", readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: {}", writeLimiter.getRate());
        logger.info("PARAM -- Token range exception dir: {}", tokenRangeExceptionDir);
        logger.info("PARAM -- Token range exception file name: {}", exceptionFileName);

        this.originSession = new EnhancedSession(propertyHelper, originSession, true);
        this.targetSession = new EnhancedSession(propertyHelper, targetSession, false);
        this.originSession.getCqlTable().setOtherCqlTable(this.targetSession.getCqlTable());
        this.targetSession.getCqlTable().setOtherCqlTable(this.originSession.getCqlTable());

        boolean allFeaturesValid = true;
        for (Feature f : featureMap.values()) {
            if (!f.initializeAndValidate(this.originSession.getCqlTable(), this.targetSession.getCqlTable())) {
                allFeaturesValid = false;
                logger.error("Feature {} is not valid.  Please check the configuration.", f.getClass().getName());
            }
        }
        if (!allFeaturesValid) {
            throw new RuntimeException("One or more features are not valid.  Please check the configuration.");
        }
        this.originSession.getCqlTable().setFeatureMap(featureMap);
        this.targetSession.getCqlTable().setFeatureMap(featureMap);

        PKFactory pkFactory = new PKFactory(propertyHelper, this.originSession.getCqlTable(), this.targetSession.getCqlTable());
        this.originSession.setPKFactory(pkFactory);
        this.targetSession.setPKFactory(pkFactory);

    }



}
