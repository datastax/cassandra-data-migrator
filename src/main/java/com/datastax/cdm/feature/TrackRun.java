/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.feature;

import java.math.BigInteger;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.statement.TargetUpsertRunDetailsStatement;
import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.job.PartitionRange;
import com.datastax.cdm.job.RunNotStartedException;
import com.datastax.oss.driver.api.core.CqlSession;

public class TrackRun {
    public enum RUN_STATUS {
        NOT_STARTED, STARTED, PASS, FAIL, DIFF, DIFF_CORRECTED, ENDED
    }

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private TargetUpsertRunDetailsStatement runStatement;

    public TrackRun(CqlSession session, String keyspaceTable) {
        this.runStatement = new TargetUpsertRunDetailsStatement(session, keyspaceTable);
    }

    public Collection<PartitionRange> getPendingPartitions(long prevRunId, JobType jobType)
            throws RunNotStartedException {
        Collection<PartitionRange> pendingParts = runStatement.getPendingPartitions(prevRunId, jobType);
        logger.info("###################### {} partitions pending from previous run id {} ######################",
                pendingParts.size(), prevRunId);
        return pendingParts;
    }

    public void initCdmRun(long runId, long prevRunId, Collection<PartitionRange> parts, JobType jobType) {
        runStatement.initCdmRun(runId, prevRunId, parts, jobType);
        logger.info("###################### Run Id for this job is: {} ######################", runId);
    }

    public void updateCdmRun(long runId, BigInteger min, RUN_STATUS status, String runInfo) {
        runStatement.updateCdmRun(runId, min, status, runInfo);
    }

    public void endCdmRun(long runId, String runInfo) {
        runStatement.endCdmRun(runId, runInfo);
    }
}
