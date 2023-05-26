package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

public class DiffJobSessionFactory implements IJobSessionFactory<SplitPartitions.Partition> {
    private static DiffJobSession jobSession = null;

    public AbstractJobSession<SplitPartitions.Partition> getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (jobSession == null) {
            synchronized (DiffJobSession.class) {
                if (jobSession == null) {
                    jobSession = new DiffJobSession(originSession, targetSession, sc);
                }
            }
        }
        return jobSession;
    }
}
