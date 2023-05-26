package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

public interface IJobSessionFactory<T> {
    AbstractJobSession<T> getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc);
}
