package com.datastax.cdm.job;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.cdm.cql.CqlHelper;
import org.apache.spark.SparkConf;

public abstract class BaseJobSession {

    protected PropertyHelper propertyHelper = PropertyHelper.getInstance();
    protected CqlHelper cqlHelper;

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
        this.cqlHelper = new CqlHelper();
    }

}
