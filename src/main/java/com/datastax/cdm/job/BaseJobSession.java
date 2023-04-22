package com.datastax.cdm.job;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.cdm.feature.Feature;
import com.datastax.cdm.feature.FeatureFactory;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.properties.PropertyHelper;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.SparkConf;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseJobSession {

    public static final String THREAD_CONTEXT_LABEL = "ThreadLabel";
    protected PropertyHelper propertyHelper = PropertyHelper.getInstance();
    protected Map<Featureset, Feature> featureMap;

    // Read/Write Rate limiter
    // Determine the total throughput for the entire cluster in terms of wries/sec,
    // reads/sec
    // then do the following to set the values as they are only applicable per JVM
    // (hence spark Executor)...
    // Rate = Total Throughput (write/read per sec) / Total Executors
    protected RateLimiter readLimiter;
    protected RateLimiter writeLimiter;
    protected Integer maxRetries = 10;

    protected Integer printStatsAfter = 100000;

    protected BaseJobSession(SparkConf sc) {
        propertyHelper.initializeSparkConf(sc);
        this.featureMap = calcFeatureMap(propertyHelper);
        ThreadContext.put(THREAD_CONTEXT_LABEL, getThreadLabel());
    }

    private Map<Featureset, Feature> calcFeatureMap(PropertyHelper propertyHelper) {
        Map<Featureset, Feature> rtn = new HashMap<>();
        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = FeatureFactory.getFeature(f); // FeatureFactory throws an RTE if the feature is not implemented
            if (feature.loadProperties(propertyHelper)) {
                rtn.put(f, feature);
            }
        }
        return rtn;
    }

    protected String getThreadLabel() {
        return ThreadContext.get("main");
    }

    protected String getThreadLabel(BigInteger min, BigInteger max) {
        String minString = min.toString();
        String maxString = max.toString();
        int minWidth = 20;
        int formattedMaxWidth = Math.max(Math.max(minString.length(), maxString.length()), minWidth);

        String formattedMin = String.format("%-" + minWidth + "s", minString).trim();
        String formattedMax = String.format("%" + formattedMaxWidth + "s", maxString);

        return formattedMin + ":" + formattedMax;
    }

}
