package com.datastax.cdm.job;

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.feature.Guardrail;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJobSession<T> extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected EnhancedSession originSession;
    protected EnhancedSession targetSession;
    protected Guardrail guardrailFeature;
    protected boolean guardrailEnabled;
    protected String tokenRangeExceptionDir;
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
            logger.warn(KnownProperties.PRINT_STATS_AFTER + " must be greater than 0.  Setting to default value of " + KnownProperties.getDefaultAsString(KnownProperties.PRINT_STATS_AFTER));
            propertyHelper.setProperty(KnownProperties.PRINT_STATS_AFTER, KnownProperties.getDefault(KnownProperties.PRINT_STATS_AFTER));
            printStatsAfter = propertyHelper.getInteger(KnownProperties.PRINT_STATS_AFTER);
        }

        rateLimiterOrigin = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_RATELIMIT_ORIGIN));
        rateLimiterTarget = RateLimiter.create(propertyHelper.getInteger(KnownProperties.PERF_RATELIMIT_TARGET));
        maxRetries = propertyHelper.getInteger(KnownProperties.MAX_RETRIES);

        tokenRangeExceptionDir = propertyHelper.getString(KnownProperties.TOKEN_RANGE_EXCEPTION_DIR);

        logger.info("PARAM -- Max Retries: {}", maxRetries);
        logger.info("PARAM -- Token range exception dir: {}", tokenRangeExceptionDir);
        logger.info("PARAM -- Origin Rate Limit: {}", rateLimiterOrigin.getRate());
        logger.info("PARAM -- Target Rate Limit: {}", rateLimiterTarget.getRate());

        this.originSession = new EnhancedSession(propertyHelper, originSession, true);
        this.targetSession = new EnhancedSession(propertyHelper, targetSession, false);
        this.originSession.getCqlTable().setOtherCqlTable(this.targetSession.getCqlTable());
        this.targetSession.getCqlTable().setOtherCqlTable(this.originSession.getCqlTable());
        this.originSession.getCqlTable().setFeatureMap(featureMap);
        this.targetSession.getCqlTable().setFeatureMap(featureMap);

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

        PKFactory pkFactory = new PKFactory(propertyHelper, this.originSession.getCqlTable(), this.targetSession.getCqlTable());
        this.originSession.setPKFactory(pkFactory);
        this.targetSession.setPKFactory(pkFactory);

        // Guardrail is referenced by many jobs, and is evaluated against the target table
        this.guardrailFeature = (Guardrail) this.targetSession.getCqlTable().getFeature(Featureset.GUARDRAIL_CHECK);
        this.guardrailEnabled = this.guardrailFeature.isEnabled();
    }

    public abstract void processSlice(T slice);

    public abstract void printCounts(boolean isFinal);
}
