package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

public class CopyPKJobSessionFactory implements IJobSessionFactory<SplitPartitions.PKRows> {
    private static CopyPKJobSession jobSession = null;

    public AbstractJobSession<SplitPartitions.PKRows> getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (jobSession == null) {
            synchronized (CopyPKJobSession.class) {
                if (jobSession == null) {
                    jobSession = new CopyPKJobSession(originSession, targetSession, sc);
                }
            }
        }
        return jobSession;
    }
}
