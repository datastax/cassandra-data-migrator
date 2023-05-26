package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

public class CopyJobSessionFactory implements IJobSessionFactory<SplitPartitions.Partition> {
    private static CopyJobSession jobSession = null;

    public AbstractJobSession<SplitPartitions.Partition> getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (jobSession == null) {
            synchronized (CopyJobSession.class) {
                if (jobSession == null) {
                    jobSession = new CopyJobSession(originSession, targetSession, sc);
                }
            }
        }
        return jobSession;
    }
}
