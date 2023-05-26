package com.datastax.cdm.job;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.SparkConf;

public class GuardrailCheckJobSessionFactory implements IJobSessionFactory<SplitPartitions.Partition> {
    private static GuardrailCheckJobSession jobSession = null;

    public AbstractJobSession<SplitPartitions.Partition> getInstance(CqlSession originSession, CqlSession targetSession, SparkConf sc) {
        if (jobSession == null) {
            synchronized (GuardrailCheckJobSession.class) {
                if (jobSession == null) {
                    jobSession = new GuardrailCheckJobSession(originSession, targetSession, sc);
                }
            }
        }
        return jobSession;
    }
}
