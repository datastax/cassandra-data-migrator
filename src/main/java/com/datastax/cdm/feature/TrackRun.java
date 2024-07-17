package com.datastax.cdm.feature;

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

import java.math.BigInteger;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.statement.TargetUpsertRunDetailsStatement;
import com.datastax.cdm.job.SplitPartitions;
import com.datastax.oss.driver.api.core.CqlSession;

public class TrackRun {
	public enum RUN_TYPE {
		MIGRATE, DIFF_DATA
	}	
	public enum RUN_STATUS {
		NOT_STARTED, STARTED, PASS, FAIL, DIFF
	}

	public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	private TargetUpsertRunDetailsStatement runStatement;

	public TrackRun(CqlSession session, String keyspacetable) {
		this.runStatement = new TargetUpsertRunDetailsStatement(session, keyspacetable);
	}

	public Collection<SplitPartitions.Partition> getPendingPartitions(long prevRunId) {
		Collection<SplitPartitions.Partition> pendingParts = runStatement.getPendingPartitions(prevRunId);
		logger.info("###################### {} partitions pending from previous run id {} ######################",
				pendingParts.size(), prevRunId);
		return pendingParts;
	}

	public long initCdmRun(Collection<SplitPartitions.Partition> parts, RUN_TYPE runType) {
		long runId = runStatement.initCdmRun(parts, runType);
		logger.info("###################### Run Id for this job is: {} ######################", runId);

		return runId;
	}

	public void updateCdmRun(BigInteger min, RUN_STATUS status) {
		runStatement.updateCdmRun(min, status);
	}

	public void endCdmRun(String runInfo) {
		runStatement.updateCdmRunInfo(runInfo);
	}
}
